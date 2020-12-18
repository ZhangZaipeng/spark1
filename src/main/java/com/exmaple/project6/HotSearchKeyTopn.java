package com.exmaple.project6;

import com.mysql.jdbc.StringUtils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

/**
 * @Description ：
 * @Tauthor ZhangZaipeng
 */
public class HotSearchKeyTopn {

  //批次时间，Batch Interval
  private static final int STREAMING_BATCH_INTERVAL = 2;

  //设置窗口时间间隔
  private static final int  STREAMING_WINDOW_INTERVAL = STREAMING_BATCH_INTERVAL * 3;

  //设置滑动窗口时间间隔
  private static final int  STREAMING_SLIDER_INTERVAL = STREAMING_BATCH_INTERVAL * 2;

  public static void main(String[] args) throws Exception {

    SparkConf sparkConf = new SparkConf().setAppName("streaming").setMaster("local[2]");

    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(STREAMING_BATCH_INTERVAL));

    Set<String> topicsSet = new HashSet<>(Arrays.asList("test".split(",")));
    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop2:9092,hadoop4:9092,hadoop6:9092");
    kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "002");
    kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    // Create direct kafka stream with brokers and topics
    JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
        jssc,
        LocationStrategies.PreferConsistent(),
        ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

    // Get the lines, split them into words, count the words and print
    JavaDStream<String> lines = messages.map(ConsumerRecord::value);

    JavaPairDStream<String, Integer> pairDS = lines.mapToPair(new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(String s) throws Exception {
        if (s == null || StringUtils.isNullOrEmpty(s)) {
          return null;
        }

        String[] splitList = s.split(",");

        if (null == splitList[1]) {
          return null;
        }

        return new Tuple2<>(splitList[1], 1);
      }
    });

    JavaPairDStream<String, Integer> reducePair = pairDS.reduceByKeyAndWindow(Integer::sum,
        Durations.seconds(STREAMING_WINDOW_INTERVAL), Durations.seconds(STREAMING_SLIDER_INTERVAL));

    reducePair.print();

    // Start the computation
    jssc.start();
    jssc.awaitTermination();

  }

}
