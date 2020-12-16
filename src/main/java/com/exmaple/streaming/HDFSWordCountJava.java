package com.exmaple.streaming;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class HDFSWordCountJava {

  public static void main(String[] args) throws Exception {
    StreamingExamples.setStreamingLogLevels();

    SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");

    JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

    String filePath = "hdfs://hadoop1:8020/hdfs/input/wordcount";

    JavaDStream<String> lines = jssc.textFileStream(filePath);

    JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

    JavaPairDStream<String, Integer> pair = words.mapToPair(word -> new Tuple2<>(word, 1));

    JavaPairDStream<String, Integer> wordcount = pair.reduceByKey((x, y) -> (x + y));

    wordcount.print();

    jssc.start();

    jssc.awaitTermination();
  }

}
