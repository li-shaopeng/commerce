
import java.util.Date

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.DateUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object AdverStat {

  def main(args: Array[String]): Unit = {
    //获取spark连接
    val conf = new SparkConf().setMaster("local[*]").setAppName("adver")
    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))

    val kafka_brokers = ConfigurationManager.config.getString(Constants.KAFKA_BROKERS)
    val kafka_topics = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)

    //3.将kafka参数映射为map
    val kafkaParam = Map(
      "bootstrap.servers" -> kafka_brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // 创建DStream，返回接收到的输入数据
    // LocationStrategies：根据给定的主题和集群地址创建consumer
    // LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
    // ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
    // ConsumerStrategies.Subscribe：订阅一系列主题
    //adRealTimeDStream:DStream[RDD,RDD]   RDD[message]  message:key,value
    val adRealTimeDStream = KafkaUtils.createDirectStream[String, String](streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam))

    //    val stream: InputDStream[ConsumerRecord[String, String]] = adRealTimeDStream
    //    ConsumerRecord(topic = AdRealTimeLog1, partition = 0, offset = 1677, CreateTime = 1593351577272, checksum = 3324709513,
    // serialized key size = -1, serialized value size = 22, key = null, value = 1593351577272 0 0 90 2)
    //得到的是String：格式 ：timestamp province city userid adid
    val adRealTimeValueDStream: DStream[String] = adRealTimeDStream.map(consumerRecordRDD => consumerRecordRDD.value())


    //获取全部黑名单数据
    val adBlacklists = AdBlacklistDAO.findAll()
    val userIdArray = adBlacklists.map(t => t.userid)
    val adRealTimeFilterDStream: DStream[String] = adRealTimeValueDStream.transform(logRDD => {
      logRDD.filter(log => {
        val logSplit = log.split(" ")
        val userId = logSplit(3).toLong
        !userIdArray.contains(userId)
      }
      )
    }
    )

    streamingContext.checkpoint("./spark-streaming")
    adRealTimeFilterDStream.checkpoint(Seconds(10))

    //ceshi
//    adRealTimeFilterDStream.foreachRDD(rdd => rdd.foreach(println(_)))

    //需求一：实时维护黑名单
    //1593351577272 0 0 90 2
    generateDynamicBlackList(adRealTimeFilterDStream)

    //需求二：各省各城市一天中的广告点击量（实时，累计统计）
    val key2ProvinceCityCountDStream = provinceCityStat(adRealTimeFilterDStream)

    // 需求三：实时统计每天每个省份top3热门广告
    calculateProvinceTop3Ad(sparkSession,key2ProvinceCityCountDStream)

    //需求四：最近一个小时的广告点击量统计
    getRecentHourClickCount(adRealTimeFilterDStream)

    streamingContext.start()
    streamingContext.awaitTermination()

  }
  def getRecentHourClickCount(adRealTimeFilterDStream: DStream[String]) = {
    val key2TimeMinuteDStream = adRealTimeFilterDStream.map {
      log => {
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toLong
        //yyyyMMddHHmm
        val timeMinute = DateUtils.formatTimeMinute(new Date(timeStamp))
        val adid = logSplit(4)

        val key = timeMinute + "_" + adid
        (key, 1L)
      }
    }
    val key2WindowDStream = key2TimeMinuteDStream.reduceByKeyAndWindow((a:Long,b:Long)=>a + b,Minutes(60),Minutes(1))
    key2WindowDStream.foreachRDD{ rdd =>
      rdd.foreachPartition{ items =>
        //保存到数据库
        val adClickTrends = ArrayBuffer[AdClickTrend]()
        for (item <- items){
          val keySplited = item._1.split("_")
          // yyyyMMddHHmm
          val dateMinute = keySplited(0)
          val adid = keySplited(1).toLong
          val clickCount = item._2

          val date = DateUtils.formatDate(DateUtils.parseDateKey(dateMinute.substring(0, 8)))
          val hour = dateMinute.substring(8, 10)
          val minute = dateMinute.substring(10)

          adClickTrends += AdClickTrend(date,hour,minute,adid,clickCount)
        }
        AdClickTrendDAO.updateBatch(adClickTrends.toArray)
      }
    }
  }


  def calculateProvinceTop3Ad(spark: SparkSession, key2ProvinceCityCountDStream: DStream[(String, Long)]) = {
    val rowsDStream = key2ProvinceCityCountDStream.transform{
      rdd => {
        // 计算出每天各省份各广告的点击量，重新转换key
        val mappedRDD = rdd.map{case(keyStr, count) =>
          val keySplited = keyStr.split("_")
          val date = keySplited(0)
          val province = keySplited(1)
          val adid = keySplited(3).toLong
          val clickCount = count

          val key = date + "_" + province + "_" + adid
          (key, clickCount)
        }
        //根据新的key进行聚合
        val dailyAdClickCountByProvinceRDD = mappedRDD.reduceByKey(_+_)
        // 将dailyAdClickCountByProvinceRDD转换为DataFrame
        // 注册为一张临时表
        val rowsRDD = dailyAdClickCountByProvinceRDD.map{ case (keyString, count) =>
          val keySplited = keyString.split("_")
          val datekey = keySplited(0)
          val province = keySplited(1)
          val adid = keySplited(2).toLong
          val clickCount = count
          val date = DateUtils.formatDate(DateUtils.parseDateKey(datekey))

          (date, province, adid, clickCount)
        }

        import spark.implicits._
        val dailyAdClickCountByProvinceDF = rowsRDD.toDF("date","province","ad_id","click_count")

        // 将dailyAdClickCountByProvinceDF，注册成一张临时表
        dailyAdClickCountByProvinceDF.createOrReplaceTempView("tmp_daily_ad_click_count_by_prov")
        // 使用Spark SQL，通过开窗函数，获取到各省份的top3热门广告
        val sqlStr = "select date, province,ad_id,click_count from(" +
          "select date, province,ad_id,click_count, row_number() over (partition by date, province order by click_count desc) rank" +
          " from tmp_daily_ad_click_count_by_prov) t" +
          " where rank <= 3"

        val provinceTop3AdDF = spark.sql(sqlStr)
        provinceTop3AdDF.rdd
      }
    }
    //每次对数据库的操作都是在partition中
    //不直接采用DF.write.format("jdbc")是因为与数据库的连接无法序列化，又不能每个row都建立一个连接。原先是直接生成一张表，不是更新
    // 每次都是刷新出来各个省份最热门的top3广告，将其中的数据批量更新到MySQL中
    rowsDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        items => {
          // 插入数据库
          val adProvinceTop3s = ArrayBuffer[AdProvinceTop3]()
          for (item <- items){
            val date = item.getString(0)
            val province = item.getString(1)
            val adid = item.getLong(2)
            val clickCount = item.getLong(3)
            adProvinceTop3s += AdProvinceTop3(date,province,adid,clickCount)
          }
          AdProvinceTop3DAO.updateBatch(adProvinceTop3s.toArray)
        }
      }
    }

  }

  def provinceCityStat(adRealTimeFilterDStream: DStream[String]) = {
    // 计算出每5个秒内的数据中，每天每个用户每个广告的点击量
    // 通过对原始实时日志的处理   格式 ：timestamp province city userid adid
    // 将日志的格式处理成<yyyyMMdd_userid_adid, 1L>格式
    val key2ProvinceCityDStream: DStream[(String, Long)] = adRealTimeFilterDStream.map { case log => {
      val logSplit = log.split(" ")
      val date = DateUtils.formatDateKey(new Date(logSplit(0).toLong))
      val provinceId = logSplit(1)
      val cityId = logSplit(2)
      val adId = logSplit(4).toLong
      (date + "_" + provinceId + "_"+ cityId + "_" + adId, 1L)
    }
    }
    // date province city userid adid
    // date_province_city_adid，作为key；1作为value
    // 通过spark，直接统计出来全局的点击次数，在spark集群中保留一份；在mysql中，也保留一份
    // 我们要对原始数据进行map，映射成<date_province_city_adid,1>格式

    // 然后呢，对上述格式的数据，执行updateStateByKey算子:根据key保存
    // spark streaming特有的一种算子，在spark集群内存中，维护一份key的全局状态
    val key2StatDStream: DStream[(String, Long)] = key2ProvinceCityDStream.updateStateByKey[Long] {
      // values，(1,1,1,1,1,1,1,1,1,1)
      // 首先根据optional判断，之前这个key，是否有对应的状态
      (values: Seq[Long], option: Option[Long]) =>
        var count = 0L
        if (option.isDefined) {
          count = option.get
        }
        for (value <- values) {
          count += value
        }
        Some(count)
    }

    // 将计算出来的最新结果，同步一份到mysql中，以便于j2ee系统使用

    key2StatDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        items =>
          val adStatArray = new ArrayBuffer[AdStat]()
          for ((key, count) <- items) {
            val keySplit = key.split("_")
            val data = keySplit(0)
            val province = keySplit(1)
            val city = keySplit(2)
            val adid = keySplit(3).toLong
            adStatArray += AdStat(data,province,city,adid,count)
          }
          AdStatDAO.updateBatch(adStatArray.toArray)
      }
    }
    key2StatDStream
  }


  def generateDynamicBlackList(adRealTimeFilterDStream: DStream[String]) = {
    // 计算出每5个秒内的数据中，每天每个用户每个广告的点击量
    // 通过对原始实时日志的处理   格式 ：timestamp province city userid adid
    // 将日志的格式处理成<yyyyMMdd_userid_adid, 1L>格式
    val key2NumDStream: DStream[(String, Long)] = adRealTimeFilterDStream.map { case log => {
      val logSplit = log.split(" ")
      val date = DateUtils.formatDateKey(new Date(logSplit(0).toLong))
      val userId = logSplit(3).toLong
      val adId = logSplit(4).toLong
      (date + "_" + userId + "_" + adId, 1L)
      }
    }

    // 提取出日期（yyyyMMdd）、userid、adid

    // 针对处理后的日志格式，执行reduceByKey算子即可，（每个batch中）每天每个用户对每个广告的点击量
    val adRealCountDStream = key2NumDStream.reduceByKey(_ + _)

    // 源源不断的，每个5s的batch中，当天每个用户对每支广告的点击次数
    // <yyyyMMdd_userid_adid, clickCount>
    adRealCountDStream.foreachRDD {
      // 对每个分区的数据就去获取一次连接对象
      // 每次都是从连接池中获取，而不是每次都创建
      // 写数据库操作，性能已经提到最高了
      rdd =>
        rdd.foreachPartition { items =>
          val AdUserClickCountArray = new ArrayBuffer[AdUserClickCount]()
          for ((log, count) <- items) {
            val logSplit = log.split("_")
            val date = logSplit(0)
            val userId = logSplit(1).toLong
            val adId = logSplit(2).toLong
            AdUserClickCountArray.append(AdUserClickCount(date, userId, adId, count))
          }
          AdUserClickCountDAO.updateBatch(AdUserClickCountArray.toArray)
        }
    }

    // 现在我们在mysql里面，已经有了累计的每天各用户对各广告的点击量
    // 遍历每个batch中的所有记录，对每条记录都要去查询一下，这一天这个用户对这个广告的累计点击量是多少
    // 从mysql中查询
    // 查询出来的结果，如果是100，如果你发现某个用户某天对某个广告的点击量已经大于等于100了
    // 那么就判定这个用户就是黑名单用户，就写入mysql的表中，持久化
    val blacklistDStream = adRealCountDStream.filter{ case (key, count) =>
      val keySplited = key.split("_")

      // yyyyMMdd -> yyyy-MM-dd
      val date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited(0)))
      val userid = keySplited(1).toLong
      val adid = keySplited(2).toLong

      // 从mysql中查询指定日期指定用户对指定广告的点击量
      val clickCount = AdUserClickCountDAO.findClickCountByMultiKey(date, userid, adid)

      // 判断，如果点击量大于等于100，ok，那么不好意思，你就是黑名单用户
      // 那么就拉入黑名单，返回true
      if(clickCount >= 100) {
        true
      }else{
        // 反之，如果点击量小于100的，那么就暂时不要管它了
        false
      }
    }

    // blacklistDStream
    // 里面的每个batch，其实就是都是过滤出来的已经在某天对某个广告点击量超过100的用户
    // 遍历这个dstream中的每个rdd，然后将黑名单用户增加到mysql中
    // 这里一旦增加以后，在整个这段程序的前面，会加上根据黑名单动态过滤用户的逻辑
    // 我们可以认为，一旦用户被拉入黑名单之后，以后就不会再出现在这里了
    // 所以直接插入mysql即可

    // 我们在插入前要进行去重
    // yyyyMMdd_userid_adid
    // 20151220_10001_10002 100
    // 20151220_10001_10003 100
    // 10001这个userid就重复了

    // 实际上，是要通过对dstream执行操作，对其中的rdd中的userid进行全局的去重, 返回Userid
    val blacklistUseridDStream = blacklistDStream.map(item => item._1.split("_")(1).toLong)

    val distinctBlacklistUseridDStream = blacklistUseridDStream.transform( uidStream => uidStream.distinct() )
    // 到这一步为止，distinctBlacklistUseridDStream
    // 每一个rdd，只包含了userid，而且还进行了全局的去重，保证每一次过滤出来的黑名单用户都没有重复的
    //每个RDD去重了，但不同RDD呢。。。

    distinctBlacklistUseridDStream.foreachRDD{ rdd =>
      rdd.foreachPartition{ items =>
        val adBlacklists = ArrayBuffer[AdBlacklist]()
        for(item <- items)
          adBlacklists += AdBlacklist(item)

        AdBlacklistDAO.insertBatch(adBlacklists.toArray)
      }
    }
  }
}
