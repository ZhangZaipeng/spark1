package com.exmaple.common

import org.apache.spark.sql.SparkSession

object CommSparkSessionScala {

  def getSparkSession(): SparkSession = {
    val spark = SparkSession.builder()
      .appName("test")
      .master("local")
      .getOrCreate();
    spark
  }

}
