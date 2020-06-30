import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object AreaTop3Stat {

  def main(args: Array[String]): Unit = {
    //获取筛选条件
    // 获取统计任务参数【为了方便，直接从配置文件中获取，企业中会从一个调度平台获取】
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)

    // 任务的执行ID，用户唯一标示运行后的结果，用在MySQL数据库中
    val taskUUID = UUID.randomUUID().toString

    //获取spark连接
    val conf = new SparkConf().setMaster("local[*]").setAppName("AreaTop3")
    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    //第一步：获取用户点击数据
    val cityIdPidRDD = getCityAndProductInfo(sparkSession,taskParam)

    //第二步：获取地区数据
    val cityId2AreaInfoRDD: RDD[(Long, Row)] = getCityAreaInfo(sparkSession)
    //tmp_area_basic_info：("city_id", "city_name", "area", "product_id")一条数据代表一次点击
    getAreaPidInfoTable(sparkSession,cityIdPidRDD,cityId2AreaInfoRDD)

    //注册自定义函数
    sparkSession.udf.register("concat_long_string",(v1:Long, v2:String, split:String)=>{
      v1 + split + v2
    })
    sparkSession.udf.register("group_concat_distinct",new GroupConcatDistinct)
    sparkSession.udf.register("get_json_field", (json: String, field: String) => {
      val jsonObject = JSONObject.fromObject(json)
      jsonObject.getString(field)
    })

    //tmp_area_click_count:|area|product_id|click_count|city_infos|
    getAreaProductClickCountTable(sparkSession)

    //tmp_area_count_product_info   |area|city_infos|product_id|product_name|product_status|click_count|
    getAreaProductClickCountInfo(sparkSession)


    // 需求一：使用开窗函数获取各个区域内点击次数排名前3的热门商品
    val areaTop3ProductRDD = getTop3Product(sparkSession,taskUUID)

    // 将数据转换为DF，并保存到MySQL数据库
    import sparkSession.implicits._
    val areaTop3ProductDF = areaTop3ProductRDD.repartition(1).rdd.map(row =>
      AreaTop3Product(taskUUID, row.getAs[String]("area"), row.getAs[String]("area_level"),
        row.getAs[Long]("product_id"), row.getAs[String]("city_infos"),
        row.getAs[Long]("click_count"), row.getAs[String]("product_name"),
        row.getAs[String]("product_status"))
    ).toDS

    areaTop3ProductDF.write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "area_top3_product")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Overwrite)
      .save()

    sparkSession.close()
//    sparkSession.sql("select * from temp_2").show()
  }

  def getTop3Product(sparkSession: SparkSession, taskUUID: String) = {
    val sql = "select area, city_infos, product_id, product_name, product_status, click_count, " +
      " row_number() OVER(PARTITION BY area ORDER BY click_count DESC ) rank from tmp_area_count_product_info"
    sparkSession.sql(sql).createOrReplaceTempView("temp_1")


    // 华北、华东、华南、华中、西北、西南、东北
    // A级：华北、华东
    // B级：华南、华中
    // C级：西北、西南
    // D级：东北

    // case when
    // 根据多个条件，不同的条件对应不同的值
    // case when then ... when then ... else ... end

    val sql2 = "SELECT " +
      "area," +
      "CASE " +
      "WHEN area='华北' OR area='华东' THEN 'A Level' " +
      "WHEN area='华南' OR area='华中' THEN 'B Level' " +
      "WHEN area='西南' OR area='西北' THEN 'C Level' " +
      "ELSE 'D Level' " +
      "END area_level," +
      "product_id," +
      "city_infos," +
      "click_count," +
      "product_name," +
      "product_status " +
      "FROM (" +
      "temp_1) t " +
      "WHERE rank<=3"
    sparkSession.sql(sql2)
  }

  def getAreaProductClickCountInfo(sparkSession: SparkSession) = {
    //join:tmp_area_click_count tacc和 product_info pi
    // 将之前得到的各区域各商品点击次数表，product_id
    // 去关联商品信息表，product_id，product_name和product_status
    // product_status要特殊处理，0，1，分别代表了自营和第三方的商品，放在了一个json串里面
    // get_json_object()函数，可以从json串中获取指定的字段的值
    // if()函数，判断，如果product_status是0，那么就是自营商品；如果是1，那么就是第三方商品
    // area, product_id, click_count, city_infos, product_name, product_status

    // 你拿到到了某个区域top3热门的商品，那么其实这个商品是自营的，还是第三方的
    val sql = "select tacc.area, tacc.city_infos, tacc.product_id, pi.product_name, " +
      "if(get_json_field(pi.extend_info,'product_status')='0','self','others') product_status," + "tacc.click_count " +
      "from tmp_area_click_count tacc join product_info pi on tacc.product_id = pi.product_id"
    sparkSession.sql(sql).createOrReplaceTempView("tmp_area_count_product_info")
  }



  def getAreaProductClickCountTable(sparkSession: SparkSession) = {
    val sql = "select area, product_id, count(*) click_count," +
      "group_concat_distinct(concat_long_string(city_id,city_name, ':')) city_infos " + "from tmp_area_basic_info group by area, product_id"
    sparkSession.sql(sql).createOrReplaceTempView("tmp_area_click_count")
  }


  def getAreaPidInfoTable(sparkSession: SparkSession,
                          cityIdPidRDD: RDD[(Long,Long)],
                          cityId2AreaInfoRDD: RDD[(Long, Row)]) = {
    val mappedRDD = cityIdPidRDD.join(cityId2AreaInfoRDD).map {
      case (cityId, (pid, rowInfo)) => (cityId, rowInfo.getString(1), rowInfo.getString(2), pid)
    }
    // 两个函数
    // UDF：concat2()，将两个字段拼接起来，用指定的分隔符
    // UDAF：group_concat_distinct()，将一个分组中的多个字段值，用逗号拼接起来，同时进行去重
    import sparkSession.implicits._
    val df = mappedRDD.toDF("city_id", "city_name", "area", "product_id")
    // 为df创建临时表
    df.createOrReplaceTempView("tmp_area_basic_info")
  }


  def getCityAreaInfo(sparkSession: SparkSession) = {
    val cityInfo = Array((0L, "北京", "华北"),
      (1L, "上海", "华东"), (2L, "南京", "华东"), (3L, "广州", "华南"),
      (4L, "三亚", "华南"), (5L, "武汉", "华中"), (6L, "长沙", "华中"),
      (7L, "西安", "西北"), (8L, "成都", "西南"), (9L, "哈尔滨", "东北"))
    import sparkSession.implicits._
    val cityInfoDF = sparkSession.sparkContext.makeRDD(cityInfo).toDF("city_id", "city_name", "area")
    cityInfoDF.rdd.map(item => (item.getAs[Long]("city_id"), item))
  }


  def getCityAndProductInfo(sparkSession: SparkSession, taskParam: JSONObject) = {
    //获取值发生过点击的action数据，product_Id不为空-1
    val startTime = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endTime = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    val sqlStr = "select city_id, click_product_id from user_visit_action where date >='" + startTime +
      "' and date <='" + endTime + "' and click_product_id != -1"
    //makeRDD
    import sparkSession.implicits._
    sparkSession.sql(sqlStr).as[CityClickProduct].rdd.map(t=>(t.city_id,t.click_product_id))
  }

}
