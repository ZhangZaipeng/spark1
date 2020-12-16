package com.exmaple.core;

import com.exmaple.common.CommSparkContext;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * 计算两个RDD之间的笛卡尔积（即第一个RDD的每个项与第二个RDD的每个项连接）并将它们作为新的RDD返回
 */
public class CartesionJava {

  public static void main(String[] args) {
    JavaSparkContext sc = CommSparkContext.getsc();

    List<String> list1 = Arrays.asList("衣服-1", "衣服-2");
    List<String> list2 = Arrays.asList("裤子-1", "裤子-2");

    JavaRDD<String> rdd1 = sc.parallelize(list1);
    JavaRDD<String> rdd2 = sc.parallelize(list2);

    /**
     * (cheryy,ben)(cherry,leo)(herry,ben)(herry,leo)
     */
    JavaPairRDD<String, String> carteValues = rdd1.cartesian(rdd2);

    for (Tuple2<String,String> o : carteValues.collect()) {
      System.out.println(o);
    }

  }
}
