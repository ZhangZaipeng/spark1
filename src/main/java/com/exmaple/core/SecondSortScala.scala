package com.exmaple.core

import org.apache.spark.{SparkConf, SparkContext}

object SecondSortScala {
  def main(args: Array[String]): Unit = {
    val sparkConf   =
      new SparkConf().setAppName("wordcountApp").setMaster("local");

    val sc = new SparkContext(sparkConf);

    val list = Array("class1 67", "class2 89", "class1 78",
      "class2 90", "class1 99", "class3 34", "class3 89")
    val rdd = sc.parallelize(list);
    val beginSortValues = rdd.map(x => (new SecondSortKeyScala(x.split(" ")(0),
      x.split(" ")(1).toInt),x))

    val sortValues = beginSortValues.sortByKey();
    sortValues.foreach( x => System.out.println(x._2))

  }
}
