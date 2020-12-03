package com.exmaple.core;

import com.exmaple.common.CommSparkContext;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class IntersectionJava {

  public static void main(String[] args) {
    JavaSparkContext sc = CommSparkContext.getsc();
    List<String> list1 = Arrays.asList("cherry", "herry", "leo");
    List<String> list2 = Arrays.asList("ben", "cherry");
    JavaRDD<String> rdd1 = sc.parallelize(list1);
    JavaRDD<String> rdd2 = sc.parallelize(list2);

    JavaRDD intersecValues = rdd1.intersection(rdd2);
    for (Object o : intersecValues.collect()) {
      System.out.println(o);
    }
  }

}
