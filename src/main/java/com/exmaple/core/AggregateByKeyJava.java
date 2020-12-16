package com.exmaple.core;

import com.exmaple.common.CommSparkContext;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class AggregateByKeyJava {

  public static void main(String[] args) {
    JavaSparkContext sc = CommSparkContext.getsc();

    List<String> list = Arrays.asList("hadoop spark", "spark hive");

    JavaRDD<String> lines = sc.parallelize(list);

    JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String line) throws Exception {
        return Arrays.asList(line.split(" ")).iterator();
      }
    });

    JavaPairRDD<String, Integer> word = words
        .mapToPair(new PairFunction<String, String, Integer>() {
          @Override
          public Tuple2 call(String word) throws Exception {
            return new Tuple2<>(word, 1);
          }
        });

    JavaPairRDD<String, Integer> wordcount = word.aggregateByKey(0,
        new Function2<Integer, Integer, Integer>() {
          @Override
          public Integer call(Integer v1, Integer v2) throws Exception {
            return v1 + v2;
          }
        }, new Function2<Integer, Integer, Integer>() {
          @Override
          public Integer call(Integer v1, Integer v2) throws Exception {
            return v1 + v2;
          }
        });

    wordcount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
      @Override
      public void call(Tuple2<String, Integer> o) throws Exception {
        System.out.println(o._1 + " : " + o._2);
      }
    });
  }
}
