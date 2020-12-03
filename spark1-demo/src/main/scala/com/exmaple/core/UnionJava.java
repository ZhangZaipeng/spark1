package com.exmaple.core;

import com.exmaple.common.CommSparkContext;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class UnionJava {

  public static void main(String[] args) {
    JavaSparkContext sc = CommSparkContext.getsc();
    List<String> list1 = Arrays.asList("cherry", "herry");
    List<String> list2 = Arrays.asList("ben", "leo");

    JavaRDD<String> rdd1 = sc.parallelize(list1);
    JavaRDD<String> rdd2 = sc.parallelize(list2);

    JavaRDD unionValues = rdd1.union(rdd2);

    for (Object o : unionValues.collect()) {
      System.out.println(o);
    }
  }

}
