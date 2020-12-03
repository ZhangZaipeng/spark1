package com.exmaple.core

import com.exmaple.common.CommSparkContextSca

object MapPartitionsScala {

  def main(args: Array[String]): Unit = {
    val sc = CommSparkContextSca.getsc()

    val list = Array("henry", "cherry", "leo", "ben")
    val map = Map("henry" -> 99.4, "cherry" -> 79.9, "leo" -> 88.3, "ben" -> 67.5);
    val rdd = sc.parallelize(list, 2);
    val mapPartitionsValues = rdd.mapPartitions(x => {
      var list = List[Double]()
      while (x.hasNext) {
        val userName = x.next();
        val score = map.get(userName).get
        list.::=(score)
      }
      list.iterator
    }, false)

    mapPartitionsValues.foreach(x => System.out.println(x))
  }
}
