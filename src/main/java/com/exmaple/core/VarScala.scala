package com.exmaple.core

import org.apache.spark.{SparkConf, SparkContext}

object VarScala {
  def getsc(): SparkContext = {
    val sparkConf = new SparkConf().setAppName("parallelize").setMaster("local");

    val sc =  new SparkContext(sparkConf);

    return sc;
  }


  def main(args: Array[String]): Unit = {

    val list = Array(1,3,4,6)
    var sc = getsc();

    val broadcast = sc.broadcast(10);

    val rdd = sc.parallelize(list);
    val values = rdd.map(x => x * broadcast.value);
    values.foreach(x => System.out.println(x));


    val accumulator = sc.longAccumulator;
    val value = rdd.foreach(x => {
      accumulator.add(x);
    })

    System.out.println(accumulator.value)

  }
}
