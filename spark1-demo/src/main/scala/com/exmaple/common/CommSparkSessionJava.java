package com.exmaple.common;

import org.apache.spark.sql.SparkSession;

public class CommSparkSessionJava {

  public static SparkSession getSparkSession() {
    SparkSession spark = SparkSession.builder()
        .appName("test1")
        .master("local")
        .config("spark.sql.warehouse.dir", "/Users/caojinbo/Documents/spark/spark-warehouse")
        .getOrCreate();
    return spark;
  }

}
