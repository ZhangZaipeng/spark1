package com.exmaple.core

import org.apache.spark.{SparkConf, SparkContext}

object TransformationScala {

  def getsc(): SparkContext = {
    val sparkConf = new SparkConf().setAppName("parallelize").setMaster("local");

    val sc = new SparkContext(sparkConf);

    return sc;
  }

  def main(args: Array[String]): Unit = {
    //    filter()
    //    flatMap();
    //    groupBykey()
    //  reduceByKey()
    //    sortByKey()
    //    join()
    cogroup()
  }

  def cogroup(): Unit = {
    val stuList = Array(Tuple2(1, "henry"),
      Tuple2(2, "leo"),
      Tuple2(3, "chenry"),
      Tuple2(4, "lili"))
    val coreList = Array(Tuple2(1, 90),
      Tuple2(2, 88),
      Tuple2(2, 50),
      Tuple2(2, 90),
      Tuple2(3, 99),
      Tuple2(4, 100))

    val sc = getsc()

    val stuRdd = sc.parallelize(stuList)
    val coreRdd = sc.parallelize(coreList)
    val coValues = stuRdd.cogroup(coreRdd)
    coValues.foreach(x => System.out.println(x._1 + "->" + x._2._1.toList + ":" + x._2._2.toList))
  }

  def join(): Unit = {
    val stuList = Array(Tuple2(1, "henry"),
      Tuple2(2, "leo"),
      Tuple2(3, "chenry"),
      Tuple2(4, "lili"));
    val coreList = Array(Tuple2(1, 90),
      Tuple2(2, 88),
      Tuple2(3, 99),
      Tuple2(4, 100));

    val sc = getsc();

    val stuRdd = sc.parallelize(stuList);
    val coreRdd = sc.parallelize(coreList);
    val joinValues = stuRdd.join(coreRdd);

    joinValues.foreach(x => {
      System.out.println(x._1 + " > " + x._2._1 + ":" + x._2._2)
    })
  }


  def sortByKey(): Unit = {
    val list = Array(Tuple2(90, "henry"),
      Tuple2(78, "leo"),
      Tuple2(88, "chenry"),
      Tuple2(99, "lili"));
    val rdd = getsc().parallelize(list);
    val sortValues = rdd.sortByKey(false);
    sortValues.foreach(x => {
      System.out.println(x._1 + " : " + x._2);
    })
  }


  def reduceByKey(): Unit = {
    val list = Array(
      Tuple2("class_1", 90),
      Tuple2("class_2", 78),
      Tuple2("class_1", 99),
      Tuple2("class_2", 90))

    val rdd = getsc().parallelize(list)
    val reduceValues = rdd.reduceByKey((x, y) => (x + y))

    /**
     * <class_1,(90+99)>
     * <class_2,(78+90)>
     */
    reduceValues.foreach(x => {
      System.out.println(x._1 + " : " + x._2)
    })
  }


  def groupBykey(): Unit = {
    val list = Array(Tuple2("class_1", 90), Tuple2("class_2", 78), Tuple2("class_1", 99), Tuple2("class_2", 90))
    val rdd = getsc().parallelize(list)
    val groupByKeyValues = rdd.groupByKey()
    groupByKeyValues.foreach(x => {
      System.out.println(x._1)
      x._2.foreach(y => System.out.println(y))
    })
  }


  def flatMap(): Unit = {
    val list = Array("hadoop hive", "hadoop hbase")
    val rdd = getsc().parallelize(list)
    val flatMapValue = rdd.flatMap(x => x.split(" "))
    flatMapValue.foreach(x => System.out.println(x))
  }

  def filter(): Unit = {
    val sparkConf = new SparkConf().setAppName("parallelize").setMaster("local");

    val sc = new SparkContext(sparkConf)

    val list = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    val rdd = sc.parallelize(list)

    val filterValue = rdd.filter(x => (x % 2 == 0))

    filterValue.foreach(x => System.out.println(x))
  }

  def map(): Unit = {
    val sparkConf = new SparkConf().setAppName("parallelize").setMaster("local");

    val sc = new SparkContext(sparkConf);

    val list = Array(1, 2, 3, 4);

    val rdd = sc.parallelize(list);

    val count = rdd.map(x => x * 2);

    count.foreach(x => System.out.println(x));


  }
}

