import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

class GroupConcatDistinct extends  UserDefinedAggregateFunction{
  //UDAF,相当于一片缓冲区，有不同索引，每个索引为一个Field
  //输入类型，集合
  override def inputSchema: StructType = StructType(StructField("cityInfo",StringType)::Nil)
  //缓冲区中的类型
  override def bufferSchema: StructType = StructType(StructField("bufferCityInfo",StringType)::Nil)
    //输出
  override def dataType: DataType = StringType

  override def deterministic: Boolean = true
  //需要完成的工作是聚合不同cityInfo，去重：cityId1:name1,cityId2:name2,...
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }
  //同个组内聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var buffer1 = buffer.getString(0)
    val input1 = input.getString(0)
    if(!buffer1.contains(input1)){
      if("".equals(buffer1))
        buffer1 += input1
      else
        buffer1 += "," +input1
      buffer.update(0,buffer1)
    }
  }
  //不同组间聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //遍历去重
    val b2CityInfo = buffer2.getString(0)
    var b1CityInfo = buffer1.getString(0)
    for(cityInfo <- b2CityInfo.split(",")){
      if(!b1CityInfo.contains(cityInfo)){
        if("".equals(b1CityInfo))
          b1CityInfo += cityInfo
        else
          b1CityInfo += "," +cityInfo
      }

    }
    buffer1.update(0,b1CityInfo)
  }
  //最终输出
  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
  }
}
