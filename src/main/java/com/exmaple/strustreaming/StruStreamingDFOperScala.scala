package com.exmaple.strustreaming

import java.sql.Date

import com.exmaple.common.CommSparkSessionScala
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row};

object StruStreamingDFOperScala {

  case class DeviceData(device: String, deviceType: String, signal: Double, deviceTime: Date)

  def main(args: Array[String]): Unit = {

    val spark = CommSparkSessionScala.getSparkSession()

    import spark.implicits._

    val socketDF = spark
      .readStream
      .format("socket")
      .option("host", "bigdata-pro-m01.kfk.com")
      .option("port", 9999)
      .load().as[String]


    val userSchema = new StructType()
      .add("device", "string")
      .add("deviceType", "string")
      .add("signal", "double")
      .add("deviceTime", "date")

    val df_device = socketDF.map(x => {
      val lines = x.split(",")
      Row(lines(0), lines(1), lines(2), lines(3))
    })(RowEncoder(userSchema))

    val df: Dataset[DeviceData] = df_device.as[DeviceData]
    val df_final = df.groupBy("deviceType").count()

    val query = df_final.writeStream
      .outputMode("update")
      .format("console")
      .start()
    query.awaitTermination()
  }

}
