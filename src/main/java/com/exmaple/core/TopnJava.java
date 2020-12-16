package com.exmaple.core;

import com.exmaple.common.CommSparkContext;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class TopnJava {

  public static void main(String[] args) {
    JavaSparkContext sc = CommSparkContext.getsc();
    List<Integer> list = Arrays.asList(23, 12, 56, 44, 23, 99, 13, 57);
    /**
     * 23,12,56,44
     * map        ->  (23,23) (12,12)(56,56)
     * sortByKey  ->  (56,56)  (23,23) (12,12)
     * map        ->  56,23,12
     * task(2)    ->  56,23  (List)
     * for()
     */
    JavaRDD<Integer> rdd = sc.parallelize(list);
    JavaPairRDD<Integer, Integer> beginSort = rdd.mapToPair(new PairFunction<Integer, Integer, Integer>() {
      @Override
      public Tuple2 call(Integer value) throws Exception {
        return new Tuple2<>(value, value);
      }
    });

    JavaPairRDD sortValues = beginSort.sortByKey(false);
    JavaRDD beginTop = sortValues.map(new Function<Tuple2<Integer, Integer>, Integer>() {
      @Override
      public Integer call(Tuple2<Integer, Integer> o) throws Exception {
        return o._2;
      }
    });
    List<Integer> topn = beginTop.take(3);
    for (int value : topn) {
      System.out.println(value);
    }
  }
}
