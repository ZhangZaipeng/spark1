package com.exmaple.core;

import com.exmaple.common.CommSparkContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * 也是按照分区进行的map操作，不过mapPartitionsWithIndex传入的参数多了一个表示分区索引的值
 */
public class MapParitionsWithIndexJava {

  public static void main(String[] args) {
    JavaSparkContext sc = CommSparkContext.getsc();
    List<String> list = Arrays.asList("henry", "cherry", "leo", "ben");

    JavaRDD<String> rdd = sc.parallelize(list, 3);

    final JavaRDD<String> indexValues = rdd
        .mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
          @Override
          public Iterator<String> call(Integer index, Iterator iterator) throws Exception {
            List<String> list = new ArrayList<>();
            while (iterator.hasNext()) {
              String _indexStr = iterator.next() + ":" + (index + 1);
              list.add(_indexStr);
            }
            return list.iterator();
          }
        }, false);

    indexValues.foreach(new VoidFunction<String>() {
      @Override
      public void call(String o) throws Exception {
        System.out.println(o);
      }
    });
  }
}
