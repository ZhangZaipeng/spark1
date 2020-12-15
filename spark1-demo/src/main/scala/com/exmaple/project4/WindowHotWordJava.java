package com.exmaple.project4;

import com.exmaple.common.CommSparkContext;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class WindowHotWordJava {

  public static void main(String[] args) throws Exception {

    /***
     * 输入数据模型  : hadoop
     *               spark
     *               hbase
     */
    JavaStreamingContext jssc = CommSparkContext.getJssc();

    JavaReceiverInputDStream<String> inputDStream = jssc.socketTextStream("192.172.1.40", 9999);

    /**
     * <hbase,1>
     */
    JavaPairDStream<String, Integer> pair = inputDStream.mapToPair(line -> {
      return new Tuple2<>(line, 1);
    });

    JavaPairDStream<String, Integer> windowWordCount = pair.reduceByKeyAndWindow((x, y) -> (x + y),
        Durations.seconds(10), Durations.seconds(5));

    /**
     * <hbase,3>
     * <spark,5>
     * <java,1>
     * <hive,10>
     */
    JavaDStream<Tuple2<String, Integer>> finalStream = windowWordCount.transform(line -> {

      JavaPairRDD<Integer, String> beginSort = line.mapToPair(x -> {
        return new Tuple2<>(x._2, x._1);
      });

      JavaPairRDD<Integer, String> sortRDD = beginSort.sortByKey(false);
      List<Tuple2<String, Integer>> windowList =
          sortRDD.mapToPair(x -> new Tuple2<>(x._2, x._1)).take(3);

      return jssc.sparkContext().parallelize(windowList);
    });

    finalStream.print();
    jssc.start();
    jssc.awaitTermination();

  }
}
