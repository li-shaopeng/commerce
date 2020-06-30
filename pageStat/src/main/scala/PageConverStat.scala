import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object PageConverStat {


  def main(args: Array[String]): Unit = {
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)
    // 任务的执行ID，用户唯一标示运行后的结果，用在MySQL数据库中
    val taskUUID = UUID.randomUUID().toString

    //获取spark连接
    val conf = new SparkConf().setMaster("local[*]").setAppName("pageConvert")
    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    //筛选合适时间的数据
    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = getUserVisitAction(sparkSession, taskParam)
    //获得目标页面切片：1_2,2_3,...
    val pageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW).split(",")
    val targetPageSplit: Array[String] = pageFlow.slice(0, pageFlow.length - 1).zip(pageFlow.tail).map {
      case (page1, page2) => page1 + "_" + page2
    }

    //需求5，获取用户行为数据
    //筛选转化数据，详细说明及格式介绍
    //根据sessionId进行组合
    val sessionId2ActionFliterRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey()

    val PageSplit2OneRDD = sessionId2ActionFliterRDD.flatMap {
      case (sid, iterableAction) => {
        //按时间对行为数据排序
        val sortAction: List[UserVisitAction] = iterableAction.toList.sortWith((action1, action2) => DateUtils.before(action1.action_time, action2.action_time))
        //提取页面流路径
        val sortPageId: List[Long] = sortAction.map(t => t.page_id)
        //转换成pageSplit
        val pageSplit = sortPageId.slice(0, sortPageId.length - 1).zip(sortPageId.tail).map {
          case (page1, page2) => page1 + "_" + page2
        }
        //过滤，保留在targetPageSplit中的页面切片转换成word count形式
        val PageSplit2One = pageSplit.filter(t => targetPageSplit.contains(t)).map((_, 1L))
        //最后返回List[(String, Int)]
        PageSplit2One
      }
    }
    //countbykey进行聚合
    val pageSplitCount: collection.Map[String, Long] = PageSplit2OneRDD.countByKey()

    //先进行start Page计算
    val startPage = pageFlow(0).toLong
    val startPageCount = sessionId2ActionRDD.filter {
      case (sid, action) => action.page_id == startPage
    }.count()
    //进行页面转换计算
    getPageConvert(sparkSession, taskUUID, pageSplitCount, targetPageSplit, startPageCount)

  }

  def getPageConvert(sparkSession: SparkSession, taskUUID: String, pageSplitCount: collection.Map[String, Long],
                     targetPageSplit: Array[String], startPageCount: Long) = {
    var beforePageCount = startPageCount.toDouble

    val convertRateMap = new mutable.HashMap[String, Double]()
    //迭代算出页面单跳转化率
    //pageSplit:(2_3,count),
    for (pageSplit <- targetPageSplit) {
      if (pageSplitCount.contains(pageSplit)) {
        //还要考虑除0
        if (beforePageCount != 0) {
          val convertRate = pageSplitCount(pageSplit).toDouble / beforePageCount
          convertRateMap += ((pageSplit, convertRate))
          beforePageCount = pageSplitCount(pageSplit).toDouble
        } else {
          convertRateMap += ((pageSplit, 0))
          beforePageCount = pageSplitCount(pageSplit).toDouble
        }
      } else {
        beforePageCount = 0
        convertRateMap += ((pageSplit, 0))
      }
    }
    //写入相应样例类中并写入MySQL
    // 重新整理数据
    val convertRates: mutable.Iterable[PageSplitConvertRate] = convertRateMap.map { case (str, rate) =>
      PageSplitConvertRate(taskUUID, str + ":" + rate)
    }

    // 将结果以重写方式写入到MySQL中
    import sparkSession.implicits._
    val pageConverRatesRDD: RDD[PageSplitConvertRate] = sparkSession.sparkContext.makeRDD(convertRates.toArray)
    pageConverRatesRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "pageConverRates")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Overwrite)
      .save()
  }



  def getUserVisitAction(sparkSession: SparkSession, taskParam: JSONObject) = {
    //获取Json对象
    val starTime = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endTime = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    //查询hive数据
    val sqlStr:String = "select * from user_visit_action where date>= '" + starTime + "' and date<='"+ endTime + "'"

    import sparkSession.implicits._
    sparkSession.sql(sqlStr).as[UserVisitAction].rdd.map(item => (item.session_id, item))

  }

}
