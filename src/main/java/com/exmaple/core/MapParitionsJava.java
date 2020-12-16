package com.exmaple.core;

import com.exmaple.common.CommSparkContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * mapPartition可以这么理解，先对RDD进行partition，再把每个partition进行map函数。
 */
public class MapParitionsJava {

  public static void main(String[] args) {
    JavaSparkContext sc = CommSparkContext.getsc();

    List<String> list = Arrays.asList("henry", "cherry", "leo", "ben");
    JavaRDD rdd = sc.parallelize(list, 4);
    final Map<String, Double> map = new HashMap<>();
    map.put("henry", 99.4);
    map.put("cherry", 79.9);
    map.put("leo", 88.3);
    map.put("ben", 67.5);

    JavaRDD mapPartition = rdd.mapPartitions(new FlatMapFunction<Iterator, Double>() {
      @Override
      public Iterator<Double> call(Iterator iterator) throws Exception {
        List<Double> list = new ArrayList();
        while (iterator.hasNext()){
          String userName =String.valueOf(iterator.next());
          Double score = map.get(userName).doubleValue();
          list.add(score);
        }
        return list.iterator();
      }
    });

    mapPartition.foreach(new VoidFunction() {
      @Override
      public void call(Object o) throws Exception {
        System.out.println(o);
      }
    });

  }
}
