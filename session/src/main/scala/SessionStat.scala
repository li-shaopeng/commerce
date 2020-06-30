import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object SessionStat {

  def main(args: Array[String]): Unit = {
    //获取筛选条件
    // 获取统计任务参数【为了方便，直接从配置文件中获取，企业中会从一个调度平台获取】
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)

    // 任务的执行ID，用户唯一标示运行后的结果，用在MySQL数据库中
    val taskUUID = UUID.randomUUID().toString

    //获取spark连接
    val conf = new SparkConf().setMaster("local[*]").setAppName("session")
    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()


    //首先要从user_visit_action的Hive表中，查询出来指定日期范围内的行为数据
    val actionRDD: RDD[UserVisitAction] = getOriActionRDD(sparkSession, taskParam)
    val sessionId2Action: RDD[(String, UserVisitAction)] = actionRDD.map(item=>(item.session_id,item))
    // 将用户行为信息转换为 K-V 结构, 并根据session_id 聚合，这就获得每天的session数量
    val session2Group: RDD[(String, Iterable[UserVisitAction])] = sessionId2Action.groupByKey()
    session2Group.cache()

    // 将数据转换为Session粒度， 格式为<sessionid,(sessionid|searchKeywords|clickCategoryIds|age|professional|city|sex)>
    val sessionId2AggrInfoRDD: RDD[(String, String)] = getSessionFullInfo(sparkSession, session2Group)

    // 注册自定义累加器
    val accumulator = new SessioAccumulator()
    sparkSession.sparkContext.register(accumulator)

    // 根据查询任务的配置，过滤用户的行为数据，同时在过滤的过程中，对累加器中的数据进行统计；符合要求的session
    val sessionId2FilterRdd: RDD[(String, String)] = getSessionFilteredRDD(taskParam, sessionId2AggrInfoRDD,accumulator)
    //执行算子
//    sessionId2FilterRdd.foreach(println(_))
    // 业务功能一：统计各个范围的session占比，并写入MySQL
    //    getSessionRatio(sparkSession, taskUUID, accumulator.value)

    //需求2，session统计 随机抽取
//    sessionRandomExtract(sparkSession,taskUUID, sessionId2FilterRdd)

    //需求3，热门品类。二次排序
    //过滤符合条件的action数据
    val sessionId2FilterActionRDD = sessionId2Action.join(sessionId2FilterRdd).map{
      case (sessionId, (action, fullInfo)) =>(sessionId, action)
    }
    //Array[(sortKey,countInfo)]
    val top10Array = top10PopularCategories(sparkSession,taskUUID,sessionId2FilterActionRDD)

    //需求4：Top10热门品类的Top10活跃Session统计
    top10ActiveSession(sparkSession,taskUUID,sessionId2FilterActionRDD, top10Array)

  }

  //需求4：Top10热门品类的Top10活跃Session统计
  def top10ActiveSession(sparkSession: SparkSession, taskUUID: String,
                         sessionId2FilterActionRDD: RDD[(String, UserVisitAction)],
                         top10Array:Array[(CategorySortKey,String)]): Unit = {
    // 第一步：将top10热门品类的id，生成一份RDD

    // 获得所有需要求的category集合
    val top10CategoryIdRDD = sparkSession.sparkContext.makeRDD(top10Array.map { case (categorySortKey, line) =>
      val categoryid = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_CATEGORY_ID).toLong;
      (categoryid, categoryid)
    })

    // 第二步：计算top10品类被各session点击的次数

    // sessionid2ActionRDD是符合过滤(职业、年龄等)条件的完整数据
    // sessionid2detailRDD ( sessionId, userAction )
    val sessionid2ActionsRDD = sessionId2FilterActionRDD.groupByKey()

    // 获取每个品类被每一个Session点击的次数
    val categoryid2sessionCountRDD = sessionid2ActionsRDD.flatMap { case (sessionid, userVisitActions) =>
      val categoryCountMap = new mutable.HashMap[Long, Long]()
      // userVisitActions中聚合了一个session的所有用户行为数据
      // 遍历userVisitActions是提取session中的每一个用户行为，并对每一个用户行为中的点击事件进行计数
      for (userVisitAction <- userVisitActions) {

        // 如果categoryCountMap中尚不存在此点击品类，则新增品类
        if (!categoryCountMap.contains(userVisitAction.click_category_id))
          categoryCountMap.put(userVisitAction.click_category_id, 0)

        // 如果categoryCountMap中已经存在此点击品类，则进行累加
        if (userVisitAction.click_category_id != -1L) {
          categoryCountMap.update(userVisitAction.click_category_id, categoryCountMap(userVisitAction.click_category_id) + 1)
        }
      }

      // 对categoryCountMap中的数据进行格式转化
      for ((categoryid, count) <- categoryCountMap)
        yield (categoryid, sessionid + "," + count)
    }

    // 通过top10热门品类top10CategoryIdRDD与完整品类点击统计categoryid2sessionCountRDD进行join，仅获取热门品类的数据信息
    // 获取到to10热门品类，被各个session点击的次数【将数据集缩小】
    val top10CategorySessionCountRDD = top10CategoryIdRDD.join(categoryid2sessionCountRDD).map { case (cid, (ccid, value)) =>
      (cid, value) }


    // 第三步：分组取TopN算法实现，获取每个品类的top10活跃用户

    // 先按照品类分组
    val top10CategorySessionCountsRDD = top10CategorySessionCountRDD.groupByKey(1)

    // 将每一个品类的所有点击排序，取前十个，并转换为对象
    val top10SessionObjectRDD = top10CategorySessionCountsRDD.flatMap { case (categoryid, clicks) =>
      // 先排序，然后取前10
      val top10Sessions = clicks.toList.sortWith(_.split(",")(1) > _.split(",")(1)).take(10)
      // 重新整理数据
      top10Sessions.map { case line =>
        val sessionid = line.split(",")(0)
        val count = line.split(",")(1).toLong
        Top10Session(taskUUID, categoryid, sessionid, count)
      }
    }

    // 将结果以重写方式写入到MySQL中
    import sparkSession.implicits._
    top10SessionObjectRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "top10_session")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Overwrite)
      .save()

  }


  def getClickCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {

    // 只将点击行为过滤出来
    val clickActionRDD = sessionid2detailRDD.filter { case (sessionid, userVisitAction) => userVisitAction.click_category_id != -1L }
    // 获取每种类别的点击次数
    // map阶段：(品类ID，1L)
    val clickCategoryIdRDD = clickActionRDD.map { case (sessionid, userVisitAction) => (userVisitAction.click_category_id, 1L) }
    // 计算各个品类的点击次数
    // reduce阶段：对map阶段的数据进行汇总
    clickCategoryIdRDD.reduceByKey(_ + _)
  }

  def getOrderCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {
    // 过滤订单数据
    val orderActionRDD = sessionid2detailRDD.filter { case (sessionid, userVisitAction) => userVisitAction.order_category_ids != null }
    // 获取每种类别的下单次数
    val orderCategoryIdRDD = orderActionRDD.flatMap { case (sessionid, userVisitAction) => userVisitAction.order_category_ids.split(",").map(item => (item.toLong, 1L)) }
    // 计算各个品类的下单次数
    orderCategoryIdRDD.reduceByKey(_ + _)
  }


  def getPayCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {
    // 过滤支付数据
    val payActionRDD = sessionid2detailRDD.filter { case (sessionid, userVisitAction) => userVisitAction.pay_category_ids != null }
    // 获取每种类别的支付次数
    val payCategoryIdRDD = payActionRDD.flatMap { case (sessionid, userVisitAction) => userVisitAction.pay_category_ids.split(",").map(item => (item.toLong, 1L)) }
    // 计算各个品类的支付次数
    payCategoryIdRDD.reduceByKey(_ + _)
  }


  def joinCategoryAndData(categoryidRDD: RDD[(Long, Long)],
                          clickCategoryId2CountRDD: RDD[(Long, Long)],
                          orderCategoryId2CountRDD: RDD[(Long, Long)],
                          payCategoryId2CountRDD: RDD[(Long, Long)]) = {

    // 将所有品类信息与点击次数信息结合【左连接】
    val clickJoinRDD = categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD).map { case (categoryid, (cid, optionValue)) =>
      val clickCount = if (optionValue.isDefined) optionValue.get else 0L
      val value = Constants.FIELD_CATEGORY_ID + "=" + categoryid + "|" + Constants.FIELD_CLICK_COUNT + "=" + clickCount
      (categoryid, value)
    }

    // 将所有品类信息与订单次数信息结合【左连接】
    val orderJoinRDD = clickJoinRDD.leftOuterJoin(orderCategoryId2CountRDD).map { case (categoryid, (ovalue, optionValue)) =>
      val orderCount = if (optionValue.isDefined) optionValue.get else 0L
      val value = ovalue + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount
      (categoryid, value)
    }

    // 将所有品类信息与付款次数信息结合【左连接】
    val payJoinRDD = orderJoinRDD.leftOuterJoin(payCategoryId2CountRDD).map { case (categoryid, (ovalue, optionValue)) =>
      val payCount = if (optionValue.isDefined) optionValue.get else 0L
      val value = ovalue + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount
      (categoryid, value)
    }
    payJoinRDD
  }
  //需求3/top10品类
  def top10PopularCategories(sparkSession: SparkSession, taskUUID: String,
                             sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) ={
    // 第一步：获取每一个Sessionid 点击过、下单过、支付过的数量

    // 获取所有产生过点击、下单、支付中任意行为的商品类别,唯一
    var categoryidRDD: RDD[(Long, Long)] = sessionId2FilterActionRDD.flatMap {
      case (sid, action) => {
        var categoryBuffer: ArrayBuffer[(Long, Long)] = new ArrayBuffer[(Long, Long)]()
        if (action.click_category_id != -1L)
          categoryBuffer += ((action.click_category_id, action.click_category_id))
        else if (action.order_category_ids != null)
          for (ac <- action.order_category_ids.split(",")) {
            categoryBuffer += ((ac.toLong, ac.toLong))
          }
        // 一个session中支付的商品ID集合
        else if (action.pay_category_ids != null) {
          for (payCategoryId <- action.pay_category_ids.split(","))
            categoryBuffer += ((payCategoryId.toLong, payCategoryId.toLong))
        }
        categoryBuffer
      }
    }
    categoryidRDD = categoryidRDD.distinct()
    // 第二步：计算各品类的点击、下单和支付的次数

    // 计算各个品类的点击次数
    val clickCategoryId2CountRDD = getClickCategoryId2CountRDD(sessionId2FilterActionRDD)
    // 计算各个品类的下单次数
    val orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(sessionId2FilterActionRDD)
    // 计算各个品类的支付次数
    val payCategoryId2CountRDD = getPayCategoryId2CountRDD(sessionId2FilterActionRDD)

    //第三步，联合各品类的点击下单支付 (categoryid, value)
    val categoryid2countRDD = joinCategoryAndData(categoryidRDD, clickCategoryId2CountRDD, orderCategoryId2CountRDD, payCategoryId2CountRDD)

    // 第四步：自定义二次排序key

    // 第五步：将数据映射成<CategorySortKey,info>格式的RDD，然后进行二次排序（降序）
    // 创建用于二次排序的联合key —— (CategorySortKey(clickCount, orderCount, payCount), line)
    // 按照：点击次数 -> 下单次数 -> 支付次数 这一顺序进行二次排序
    val sortKey2countRDD: RDD[(CategorySortKey, String)] = categoryid2countRDD.map {
      case (categoryid, line) => {
        val clickCount = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_PAY_COUNT).toLong
        (CategorySortKey(clickCount, orderCount, payCount), line)
      }
    }
    val sortedCategoryCountRDD = sortKey2countRDD.sortByKey(false)
    // 第六步：用take(10)取出top10热门品类，并写入MySQL
    // 引入隐式转换，准备进行RDD向Dataframe的转换
    import sparkSession.implicits._
    // 为了方便地将数据保存到MySQL数据库，将RDD数据转换为Dataframe
    val top10CategoryList = sortedCategoryCountRDD.take(10)
    val top10Category = top10CategoryList.map { case (categorySortKey, line) =>
      val categoryid = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_CATEGORY_ID).toLong
      val clickCount = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_CLICK_COUNT).toLong
      val orderCount = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_ORDER_COUNT).toLong
      val payCount = StringUtils.getFieldFromConcatString(line, "\\|", Constants.FIELD_PAY_COUNT).toLong

      Top10Category(taskUUID, categoryid, clickCount, orderCount, payCount)
    }
    // 将Map结构转化为RDD
    val top10CategoryRDD = sparkSession.sparkContext.makeRDD(top10Category, 1)

    // 写入MySQL之前，将RDD转化为Dataframe
    import sparkSession.implicits._
    top10CategoryRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "top10_category")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Overwrite)
      .save()

    top10CategoryList
  }


  //需求2，session统计 随机抽取
  def sessionRandomExtract(sparkSession: SparkSession, taskUUID: String, sessionId2FilterRdd: RDD[(String, String)]): Unit = {
    //获得时间与 全信息统计
    val dateHour2Info = sessionId2FilterRdd.map { case (sid, info) => {
      //startTime=2020-06-24 18:00:00
      val getTime = StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_START_TIME)
      val strTime = getTime.split(" ")
      val dateHour = strTime(0) + "_" +strTime(1).split(":")(0)
      (dateHour, info)
    }
    }

    val dateHourCountMap: collection.Map[String, Long] = dateHour2Info.countByKey()
    val date_hour_CountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()
    //遍历每条按照小时的细粒度 进行 三维表生成：日期与时间 对应统计数
    for ((dateHour, count) <- dateHourCountMap) {
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)

      date_hour_CountMap.get(date) match {
        case None =>
          date_hour_CountMap(date) = new mutable.HashMap[String, Long]()
          date_hour_CountMap(date) += (hour -> count)
        case Some(map) => date_hour_CountMap(date) += (hour -> count)
      }
    }

    //根据每小时的数量 ，算出需要在当前小时中抽取的个数
    val extractPerDay = 100 / date_hour_CountMap.size
    // 每小时抽取的个数 = （这个小时的数量/一天的数量）* 当天抽取的数量

    //dateHour2Info中 (dateHour, info) info 是可迭代的类型,是细粒度中小时的集合
    //得到的是hour -> session.itor
    //想要得到的是抽取后的数据：
    val dateHour2IterInfo: RDD[(String, Iterable[String])] = dateHour2Info.groupByKey()

    val dateHour2ExtractInfo = dateHour2IterInfo.map {
      case (dateHour, info) => {
        val date = dateHour.split("_")(0)
        val hour = dateHour.split("_")(1)
        //当前小时Count总数
        val countDateHour = date_hour_CountMap(date)(hour)

        //当天抽取的总数
        val dayCount: Long = date_hour_CountMap(date).values.sum
        // 每小时抽取的个数 = （这个小时的数量/一天的数量）* 当天抽取的数量
        var hourCountExtract = (countDateHour / dayCount.toDouble) * extractPerDay
        //防止数据溢出
        if (hourCountExtract > countDateHour)
          hourCountExtract = countDateHour

        //随机从原info中获得hourCountExtract个数，并返回可迭代类型
        val extractInfo: Iterable[String] = Random.shuffle(info).take(hourCountExtract.round.toInt)
        (dateHour, extractInfo)
      }
    }

    //包装成SessionRandomExtract
    // 创建类似 val sessionRandomExtractArray = new ArrayBuffer[SessionRandomExtract]()
    val sessionRandomExtract: RDD[SessionRandomExtract] = dateHour2ExtractInfo.flatMap {
      case (dateHour, extractInfo) => {
        val sessionRandomExtractArray = new ArrayBuffer[SessionRandomExtract]()
        for (sessionAggrInfo <- extractInfo) {
          // 筛选出的session，则提取此sessionAggrInfo中的数据
          val sessionid = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID)
          val starttime = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_START_TIME)
          val searchKeywords = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
          val clickCategoryIds = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)
          sessionRandomExtractArray += SessionRandomExtract(taskUUID, sessionid, starttime, searchKeywords, clickCategoryIds)

        }
        sessionRandomExtractArray
      }
    }

    sessionRandomExtract.collect()
    //将返回的RDD toDF写入数据库当中
    import sparkSession.implicits._
    sessionRandomExtract.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_random_extract")
      .mode(SaveMode.Append)
      .save()

  }


  def getSessionRatio(sparkSession: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]): Unit = {
    val session_count = value.getOrElse(Constants.SESSION_COUNT,1).toDouble

    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    // 计算各个访问时长和访问步长的范围
    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    // 将统计结果封装为Domain对象
    val sessionAggrStat = SessionAggrStat(taskUUID,
      session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    import sparkSession.implicits._
    val sessionAggrStatRDD = sparkSession.sparkContext.makeRDD(Array(sessionAggrStat))
    sessionAggrStatRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "session_aggr_stat_lisp")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }


  def getSessionFilteredRDD(taskParam: JSONObject, sessionId2AggrInfoRDD: RDD[(String, String)], sessionAggrStatAccumulator: SessioAccumulator ) = {
    // 获取查询任务中的配置
    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    var _parameter = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")

    if (_parameter.endsWith("\\|")) {
      _parameter = _parameter.substring(0, _parameter.length() - 1)
    }

    val parameter = _parameter
    // 根据筛选参数进行过滤
    val filteredSessionid2AggrInfoRDD = sessionId2AggrInfoRDD.filter { case (sessionid, aggrInfo) =>
      // 按照年龄范围进行过滤（startAge、endAge）
      var success = true
      if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE))
        success = false
      // 按照职业范围进行过滤（professionals）
      if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS))
        success = false

      // 按照城市范围进行过滤（cities）
      if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES))
        success = false

      // 按照性别进行过滤
      if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX))
        success = false

      // 按照搜索词进行过滤
      // 主要判定session搜索的词中，有任何一个，与筛选条件中任何一个搜索词相当，即通过
      if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS))
        success = false

      // 按照点击品类id进行过滤
      if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS))
        success = false

      if(success){
        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT)

        // 计算访问时长范围
        def calculateVisitLength(visitLength: Long) {
          if (visitLength >= 1 && visitLength <= 3) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s)
          } else if (visitLength >= 4 && visitLength <= 6) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s)
          } else if (visitLength >= 7 && visitLength <= 9) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s)
          } else if (visitLength >= 10 && visitLength <= 30) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s)
          } else if (visitLength > 30 && visitLength <= 60) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s)
          } else if (visitLength > 60 && visitLength <= 180) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m)
          } else if (visitLength > 180 && visitLength <= 600) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m)
          } else if (visitLength > 600 && visitLength <= 1800) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m)
          } else if (visitLength > 1800) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m)
          }
        }

        // 计算访问步长范围
        def calculateStepLength(stepLength: Long) {
          if (stepLength >= 1 && stepLength <= 3) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3)
          } else if (stepLength >= 4 && stepLength <= 6) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6)
          } else if (stepLength >= 7 && stepLength <= 9) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9)
          } else if (stepLength >= 10 && stepLength <= 30) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30)
          } else if (stepLength > 30 && stepLength <= 60) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60)
          } else if (stepLength > 60) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60)
          }
        }

        // 计算出session的访问时长和访问步长的范围，并进行相应的累加
        val visitLength = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
        val stepLength = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
        calculateVisitLength(visitLength)
        calculateStepLength(stepLength)
      }
      success
    }
    filteredSessionid2AggrInfoRDD
  }


  def getSessionFullInfo(sparkSession: SparkSession,
                         session2Group: RDD[(String, Iterable[UserVisitAction])]) = {
    val User2ActionretRDD: RDD[(Long, String)] = session2Group.map {
      case (sessionId: String, iterableAction: Iterable[UserVisitAction]) => {
        var userId = -1L
        var startTime: Date = null
        var endTime: Date = null
        var stepLength = 0
        var searchKeywords = new StringBuilder("")
        var clickCategories = new StringBuilder("")

        for (action <- iterableAction) {
          if (userId == -1L) {
            userId = action.user_id
          }
          val actionTime = DateUtils.parseTime(action.action_time)
          if (startTime == null || startTime.after(actionTime)) {
            startTime = actionTime
          }
          if (endTime == null || endTime.before(actionTime)) {
            endTime = actionTime
          }
          // 实际上这里要对数据说明一下
          // 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的
          // 其实，只有搜索行为，是有searchKeyword字段的
          // 只有点击品类的行为，是有clickCategoryId字段的
          // 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的

          // 我们决定是否将搜索词或点击品类id拼接到字符串中去
          // 首先要满足：不能是null值
          // 其次，之前的字符串中还没有搜索词或者点击品类id
          val searchKeyword: String = action.search_keyword
          if (StringUtils.isNotEmpty(searchKeyword) && !searchKeywords.toString.contains(searchKeyword)) {
            searchKeywords.append(searchKeyword + ",")
          }
          val clickCategory = action.click_category_id
          if (clickCategory != -1 && !clickCategories.toString.contains(clickCategory.toString)) {
            clickCategories.append(clickCategory + ",")
          }
          // 计算session访问步长
          stepLength += 1
        }
        val searchKeys: String = StringUtils.trimComma(searchKeywords.toString)
        val clickCates: String = StringUtils.trimComma(clickCategories.toString)

        // 计算session访问时长（秒）
        val visitLength = (endTime.getTime - startTime.getTime) / 1000

        // 聚合数据，使用key=value|key=value
        val partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeys + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCates + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)
        (userId, partAggrInfo)
      }
    }
    import sparkSession.implicits._
    val sql = "select * from user_info"
    val UserInfoRDD: RDD[UserInfo] = sparkSession.sql(sql).as[UserInfo].rdd
    val UserId2Info: RDD[(Long, UserInfo)] = UserInfoRDD.map(t => (t.user_id, t))
    val JoinUserRDD: RDD[(Long, (String, UserInfo))] = User2ActionretRDD.join(UserId2Info)
    val sessionId2Aggre: RDD[(String, String)] = JoinUserRDD.map { case (usrId, (ss, userInfo)) => {
      val str: String = ss + "|" + Constants.FIELD_AGE + "=" + userInfo.age + "|" +
        Constants.FIELD_PROFESSIONAL + "=" + userInfo.professional + "|" +
        Constants.FIELD_SEX + "=" + userInfo.sex + "|" +
        Constants.FIELD_CITY + "=" + userInfo.city
      val sessionId = StringUtils.getFieldFromConcatString(ss, "\\|", Constants.FIELD_SESSION_ID)
      (sessionId, str)
    }
    }
    sessionId2Aggre
  }

  def getOriActionRDD(sparkSession: SparkSession, taskParam: JSONObject)={
    //获取Json对象
    val starTime = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endTime = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    //查询hive数据
    val sqlStr:String = "select * from user_visit_action where date>= '" + starTime + "' and date<='"+ endTime + "'"

    import sparkSession.implicits._
    sparkSession.sql(sqlStr).as[UserVisitAction].rdd
  }
}
