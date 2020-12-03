package com.exmaple.core;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class TransformationJava {

  public static JavaSparkContext getsc() {
    SparkConf sparkConf = new SparkConf().setAppName("parallelize").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    return sc;
  }

  public static void main(String[] args) {
//    map();
//    filter();
//    flatMap();
//    groupByKey();
//    reduceByKey();
//    sortByKey();
//    join();
//    cogroup();
  }

  /**
   * 数据集一 ：(2,leo) (2,leo1)                   cogroup =>  <2,<(leo,leo1),(88,90,55,78)>>
   * 数据集二：(2,88)(2,90)(2,55)(2,78)
   *
   * <Integer, Tuple2<Iterable,Iterable>>
   */
  public static void cogroup() {
    List<Tuple2<Integer, String>> stuList = Arrays.asList(
        new Tuple2<>(1, "henry"),
        new Tuple2<>(1, "henry1"),
        new Tuple2<>(2, "leo"),
        new Tuple2<>(2, "leo1"),
        new Tuple2<>(3, "chenry"),
        new Tuple2<>(3, "chenry1"),
        new Tuple2<>(4, "lili"),
        new Tuple2<>(4, "lili1"));

    List<Tuple2<Integer, Integer>> coreList = Arrays.asList(
        new Tuple2<>(1, 90),
        new Tuple2<>(2, 88),
        new Tuple2<>(2, 90),
        new Tuple2<>(2, 55),
        new Tuple2<>(2, 78),
        new Tuple2<>(3, 99),
        new Tuple2<>(4, 100));

    JavaSparkContext sc = getsc();

    JavaPairRDD<Integer, String> stuRdd = sc.parallelizePairs(stuList);
    JavaPairRDD<Integer, Integer> coreRdd = sc.parallelizePairs(coreList);
    JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> coValues = stuRdd.cogroup(coreRdd);

    coValues.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
      @Override
      public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> integerTuple2Tuple2)
          throws Exception {
        System.out.println(integerTuple2Tuple2._1);
        System.out.println(integerTuple2Tuple2._2._1 + ":" + integerTuple2Tuple2._2._2);
      }
    });

  }

  /**
   * 数据集一 ：(1,henry)    join =>   <1,<henry,90>> 数据集二：(1,90)
   */
  public static void join() {
    List<Tuple2<Integer,String>> stuList = Arrays.asList(
        new Tuple2<>(1, "henry"),
        new Tuple2<>(1, "henry1"),
        new Tuple2<>(2, "leo"),
        new Tuple2<>(3, "chenry"),
        new Tuple2<>(4, "lili"));

    List<Tuple2<Integer, Integer>> coreList = Arrays.asList(
        new Tuple2<>(1, 90),
        new Tuple2<>(1, 91),
        new Tuple2<>(2, 88),
        new Tuple2<>(3, 99),
        new Tuple2<>(4, 100));

    JavaSparkContext sc = getsc();

    JavaPairRDD<Integer,String> stuRdd = sc.parallelizePairs(stuList);
    JavaPairRDD<Integer, Integer> coreRdd = sc.parallelizePairs(coreList);

    JavaPairRDD<Integer, Tuple2<String, Integer>> joinValues = stuRdd.join(coreRdd);

    joinValues.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
      @Override
      public void call(Tuple2<Integer, Tuple2<String, Integer>> integerTuple2Tuple2)
          throws Exception {
        System.out.println(integerTuple2Tuple2._1);
        System.out.println(integerTuple2Tuple2._2._1 + " : " + integerTuple2Tuple2._2._2);
      }
    });
  }

  /**
   * <90,henry> <88,henry>    ->   <88,henry> <90,henry>
   */
  public static void sortByKey() {
    List<Tuple2<Integer,String>> list = Arrays.asList(new Tuple2<>(90, "henry"),
        new Tuple2<>(78, "leo"),
        new Tuple2<>(88, "chenry"),
        new Tuple2<>(99, "lili"));

    JavaPairRDD<Integer,String> rdd = getsc().parallelizePairs(list);

    JavaPairRDD<Integer,String> sortValues = rdd.sortByKey(true);

    sortValues.foreach(new VoidFunction<Tuple2<Integer, String>>() {
      @Override
      public void call(Tuple2 o) throws Exception {
        System.out.println(o._1 + " : " + o._2);
      }
    });
  }

  public static void reduceByKey() {
    List<Tuple2<String, Integer>> list = Arrays.asList(
        new Tuple2<>("class_1", 90),
        new Tuple2<String, Integer>("class_2", 78),
        new Tuple2<String, Integer>("class_1", 99),
        new Tuple2<String, Integer>("class_2", 90));

    JavaPairRDD<String, Integer> rdd = getsc().parallelizePairs(list);

    /**
     * <class_1,(90+99)>
     *     <class_2,(78+90)>
     */
    JavaPairRDD<String, Integer> reduceValues = rdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer v1, Integer v2) throws Exception {
        return v1 + v2;
      }
    });

    reduceValues.foreach(new VoidFunction<Tuple2<String, Integer>>() {
      @Override
      public void call(Tuple2 value) throws Exception {
        System.out.println(value._1 + " : " + value._2);
      }
    });
  }


  /**
   * class_1 90      groupbykey  <class_1,(90,99,86)>  <class_2,(78,76,90> class_2 78 class_1 99
   * class_2 76 class_2 90 class_1 86
   */
  public static void groupByKey() {
    List<Tuple2<String, Integer>> list = Arrays.asList(new Tuple2<String, Integer>("class_1", 90),
        new Tuple2<String, Integer>("class_2", 78),
        new Tuple2<String, Integer>("class_1", 99),
        new Tuple2<String, Integer>("class_2", 90));

    JavaPairRDD<String, Integer> rdd = getsc().parallelizePairs(list);

    JavaPairRDD<String, Iterable<Integer>> groupBykeyValues = rdd.groupByKey();

    groupBykeyValues.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
      @Override
      public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
        System.out.println(stringIterableTuple2._1);
        Iterator iterator = stringIterableTuple2._2.iterator();
        while (iterator.hasNext()) {
          System.out.println(iterator.next());
        }
      }
    });

  }

  public static void flatMap() {
    List<String> list = Arrays.asList("hadoop hive", "hadoop hbase");

    JavaRDD<String> rdd = getsc().parallelize(list);

    JavaRDD<String> flatMapValue = rdd.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String line) throws Exception {
        return Arrays.asList(line.split(" ")).iterator();
      }
    });

    flatMapValue.foreach(new VoidFunction<String>() {
      @Override
      public void call(String o) throws Exception {
        System.out.println(o);
      }
    });

  }


  public static void filter() {
    List list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    JavaRDD rdd = getsc().parallelize(list);
    JavaRDD filterValue = rdd.filter(new Function<Integer, Boolean>() {
      @Override
      public Boolean call(Integer value) throws Exception {
        return value % 2 == 0;
      }
    });
    filterValue.foreach(new VoidFunction() {
      @Override
      public void call(Object value) throws Exception {
        System.out.println(value);
      }
    });
  }

  public static void map() {
    SparkConf sparkConf = new SparkConf().setAppName("parallelize").setMaster("local");

    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    List list = Arrays.asList(1, 2, 3, 4);

    JavaRDD rdd = sc.parallelize(list);

    JavaRDD count = rdd.map(new Function<Integer, Integer>() {
      @Override
      public Integer call(Integer value) throws Exception {
        return value * 10;
      }
    });

    count.foreach(new VoidFunction() {
      @Override
      public void call(Object o) throws Exception {
        System.out.println(o);
      }
    });
  }

}
