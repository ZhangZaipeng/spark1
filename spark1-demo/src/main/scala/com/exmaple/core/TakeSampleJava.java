package com.exmaple.core;

import com.exmaple.common.CommSparkContext;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TakeSampleJava {

  public static void main(String[] args) {
    JavaSparkContext sc = CommSparkContext.getsc();
    List<String> list = Arrays.asList("henry", "chery", "ben", "leo", "lili");
    JavaRDD rdd = sc.parallelize(list);
    List list1 = rdd.takeSample(false, 3);
    for (Object o : list1) {
      System.out.println(o);
    }
  }
}
