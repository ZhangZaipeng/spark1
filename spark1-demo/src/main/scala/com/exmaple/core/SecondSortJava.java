package com.exmaple.core;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class SecondSortJava {

  public static void main(String[] args) {

    SparkConf sparkConf = new SparkConf().setAppName("wordCountApp").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    /**
     * class1 67,
     * class2 89,
     * class1 78,       ->  class1 (67,78,99)  class2 (89,99)
     * class2 90,
     * class1 99
     */
    List<String> list = Arrays.asList("class1 67", "class2 89", "class1 78",
        "class2 90", "class1 99", "class3 34", "class3 89");

    JavaRDD<String> rdd = sc.parallelize(list);

    JavaPairRDD benginSortValues = rdd.mapToPair(new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2 call(String line) throws Exception {
        String first = line.split(" ")[0];
        int second = Integer.parseInt(line.split(" ")[1]);
        SecondSortKey secondSortKey = new SecondSortKey(first, second);
        return new Tuple2<>(secondSortKey, line);
      }
    });

    JavaPairRDD sortValues = benginSortValues.sortByKey(false);

    sortValues.foreach(new VoidFunction<Tuple2<SecondSortKey, String>>() {
      @Override
      public void call(Tuple2 o) throws Exception {
        System.out.println(o._2);
      }
    });

  }
}
