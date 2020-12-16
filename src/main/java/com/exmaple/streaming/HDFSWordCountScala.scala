package com.exmaple.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

object HDFSWordCountScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

    val jssc = new StreamingContext(conf, Durations.seconds(5))

    val filePath: String = "hdfs://bigdata-pro-m01.kfk.com:9000/user/kfk/datas/streaming"

    val lines = jssc.textFileStream(filePath)

    val words = lines.flatMap(line => line.split(" "));

    val pair = words.map(word => (word, 1))

    val wordcount = pair.reduceByKey((x, y) => x + y)

    wordcount.print()

    jssc.start()
    jssc.awaitTermination()
  }
}
