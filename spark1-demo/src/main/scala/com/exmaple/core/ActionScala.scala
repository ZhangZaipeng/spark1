package com.exmaple.core

import org.apache.spark.{SparkConf, SparkContext}

object ActionScala {
  def getsc(): SparkContext = {
    val sparkConf = new SparkConf().setAppName("parallelize").setMaster("local");
    val sc = new SparkContext(sparkConf);
    return sc;
  }

  def main(args: Array[String]): Unit = {
    // reduce()
    // collect()
    countBykey()
  }

  def countBykey(): Unit = {
    val list = Array(Tuple2("class_1", "leo"),
      Tuple2("class_2", "henry"),
      Tuple2("class_1", "cherry"),
      Tuple2("class_1", "ben"),
      Tuple2("class_2", "lili"));

    val rdd = getsc().parallelize(list)

    val countBykeyValue = rdd.countByKey()

    System.out.println(countBykeyValue)
  }

  def save(): Unit = {
    val list = Array(1, 2, 3, 4);
    val rdd = getsc().parallelize(list)
    val values = rdd.saveAsTextFile("hdfs://bigdata-pro-m01.kfk.com:9000/user/kfk/datas/scalaRdd")
  }


  def take(): Unit = {
    val list = Array(1, 2, 3, 4);
    val rdd = getsc().parallelize(list)
    val values = rdd.take(3)
    for (value <- values) {
      System.out.println(value)
    }
  }

  def count(): Unit = {
    val list = Array(1, 2, 3, 4);
    val rdd = getsc().parallelize(list)
    val count = rdd.count()
    System.out.println(count)
  }

  def collect(): Unit = {
    val list = Array(1, 2, 3, 4);
    val rdd = getsc().parallelize(list)
    val values = rdd.collect()
    for (value <- values) {
      System.out.println(value)
    }
  }

  def reduce(): Unit = {
    val list = Array(1, 2, 3, 4);
    val rdd = getsc().parallelize(list)
    val values = rdd.reduce((x, y) => (x + y));
    System.out.println(values)
  }

}
