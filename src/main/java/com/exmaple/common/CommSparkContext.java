package com.exmaple.common;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class CommSparkContext {

  public static JavaSparkContext getsc() {
    SparkConf sparkConf = new SparkConf().setAppName("wordCountApp").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    return sc;
  }

  public static JavaStreamingContext getJssc() {
    SparkConf sparkConf = new SparkConf().setAppName("streaming").setMaster("local[2]");
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
    return jssc;
  }

}
