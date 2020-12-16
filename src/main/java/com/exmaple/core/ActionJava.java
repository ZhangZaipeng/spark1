package com.exmaple.core;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

public class ActionJava {

  public static JavaSparkContext getsc() {
    SparkConf sparkConf = new SparkConf().setAppName("parallelize").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    return sc;
  }

  public static void main(String[] args) {
    //reduce();
    //collect();
    // count();
    // take();
    //save();
    countByKey();
  }

  public static void countByKey() {
    List<Tuple2<String, String>> list = Arrays.asList(
        new Tuple2<>("class_1", "leo"),
        new Tuple2<>("class_2", "henry"),
        new Tuple2<>("class_1", "cherry"),
        new Tuple2<>("class_1", "ben"),
        new Tuple2<>("class_2", "lili"));

    JavaPairRDD<String, String> rdd = getsc().parallelizePairs(list);

    Map<String, Long> values = rdd.countByKey();

    for (Map.Entry obj : values.entrySet()) {
      System.out.println(obj.getKey() + ":" + obj.getValue());
    }
  }

  public static void save() {
    List<Integer> list = Arrays.asList(1, 2, 3, 4);
    JavaRDD<Integer> rdd = getsc().parallelize(list);
    rdd.saveAsTextFile("hdfs://bigdata-pro-m01.kfk.com:9000/user/kfk/datas/rdd");
  }

  public static void take() {
    List<Integer> list = Arrays.asList(1, 2, 3, 4);
    JavaRDD<Integer> rdd = getsc().parallelize(list);
    List<Integer> values = rdd.take(3);
    for (int value : values) {
      System.out.println(value);
    }
  }

  public static void count() {
    List<Integer> list = Arrays.asList(1, 2, 3, 4);
    JavaRDD<Integer> rdd = getsc().parallelize(list);

    System.out.println(rdd.count());
  }

  public static void collect() {
    List<Integer> list = Arrays.asList(1, 2, 3, 4);
    JavaRDD<Integer> rdd = getsc().parallelize(list);

    List<Integer> collectValues = rdd.collect();
    for (int value : collectValues) {
      System.out.println(value);
    }
  }

  public static void reduce() {
    List<Integer> list = Arrays.asList(1, 2, 3, 4);
    JavaRDD<Integer> rdd = getsc().parallelize(list);

    int count = rdd.reduce(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer integer1, Integer integer2) throws Exception {
        return integer1 + integer2;
      }
    });
    System.out.println(count);
  }

}
