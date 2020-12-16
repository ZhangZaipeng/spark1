package com.exmaple.simple

import org.apache.spark.sql.SparkSession

/**
 * @Description ：
 * @Tauthor ZhangZaipeng
 * @Tdata 2020/8/10   22:29
 */
object SimpleApp {
  def main(args: Array[String]): Unit = {
    val logFile = "file:///E:/hdfs/input/people.json" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val data = spark.read.json(logFile)
    data.show()
    implicit val encoder = org.apache.spark.sql.Encoders.STRING
    data.select(data("name"), data("age") + 1).show()
    // val numAs = logData.filter(line => line.contains("a")).count()
    // val numBs = logData.filter(line => line.contains("b")).count()
    // println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}
