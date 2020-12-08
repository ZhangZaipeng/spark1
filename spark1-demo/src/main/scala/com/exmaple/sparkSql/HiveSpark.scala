package com.exmaple.sparkSql

import org.apache.spark.sql.SparkSession

object HiveSpark {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", "/Users/caojinbo/Documents/spark/spark-warehouse")
      .config("spark.sql.inMemoryColumnarStorage.compressed", true)
      .config("spark.sql.warehouse.dir", "/Users/caojinbo/Documents/spark/spark-warehouse")
      .enableHiveSupport()
      .getOrCreate();

    spark.catalog.cacheTable("")
    spark.sql("select * from hivespark.stu_person").show()

  }
}
