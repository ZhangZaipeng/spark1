package com.exmaple.sparkSql

import java.io.File

import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object HiveSpark {

  case class HelloWorld(id:Int, name:String, age:Int, class1:String)

  def main(args: Array[String]): Unit = {

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate();

    spark.sql("select * from default.helloworld").show()

    // 写数据
    val recordsDF = spark.createDataFrame((1 to 5).map(i => HelloWorld(i, "o", 25,"09-19")))

    recordsDF.write.mode(SaveMode.Append).saveAsTable("default.helloworld")

    spark.sql("select * from default.helloworld").show()
  }
}
