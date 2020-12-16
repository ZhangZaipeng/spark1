package com.exmaple.core;

import com.exmaple.common.CommSparkContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class RePartitionJava {

  public static void main(String[] args) {
    JavaSparkContext sc = CommSparkContext.getsc();

    List<String> list = Arrays.asList("henry", "chery", "ben", "leo", "lili");
    JavaRDD<String> rdd = sc.parallelize(list, 2);
    JavaRDD<String> mapIndexValues = rdd
        .mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
          @Override
          public Iterator<String> call(Integer index, Iterator iterator) throws Exception {
            List<String> _list = new ArrayList<String>();
            while (iterator.hasNext()) {
              String userName = String.valueOf(iterator.next());
              _list.add((index + 1) + "：" + userName);
            }
            return _list.iterator();
          }
        }, false);

    JavaRDD<String> coalesceValues = mapIndexValues.repartition(3);
    JavaRDD<String> mapIndexValues2 = coalesceValues
        .mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
          @Override
          public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
            List<String> _list = new ArrayList<String>();
            while (iterator.hasNext()) {
              String userName = String.valueOf(iterator.next());
              _list.add((index + 1) + "：" + userName);
            }
            return _list.iterator();
          }
        }, false);

    for (Object o : mapIndexValues2.collect()) {
      System.out.println(o);
    }

  }
}
