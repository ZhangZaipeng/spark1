package com.exmaple.common

import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object CommSparkContextSca {

  def getsc(): SparkContext = {
    val sparkConf = new SparkConf().setAppName("parallelize").setMaster("local");
    val sc = new SparkContext(sparkConf);
    return sc;
  }

  def getJssc(): StreamingContext = {
    val sparkConf = new SparkConf().setAppName("streaming").setMaster("local[2]")
    val jssc = new StreamingContext(sparkConf, Durations.seconds(5))
    return jssc
  }

}
