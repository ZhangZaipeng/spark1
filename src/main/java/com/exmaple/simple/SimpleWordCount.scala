package com.exmaple.simple

import org.apache.spark.sql.{Encoders, SparkSession}

/**
 * @Description ：
 * @Tdata 2020/8/11   22:50
 */
object SimpleWordCount {

  case class Words(name: String, count: Int)


  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: SimpleWordCount <logFile>")
      System.exit(1)
    }

    val logFile = args(0)
    val spark = SparkSession.builder.appName("Simple WordCount").getOrCreate()

    val data = spark.read.textFile(logFile)
      .flatMap(x => x.split(" "))(Encoders.STRING)
      .map(x => (x, 1))(Encoders.product[(String,Int)])
      .map(x => Words(x._1, x._2))(Encoders.product[Words])

    data.show()

    val dataDF = data.toDF()
    dataDF.createOrReplaceTempView("words")

    val sqlDF = spark.sql("SELECT * FROM words where name = 'haha'")
    sqlDF.show()

    spark.stop()
  }

}
