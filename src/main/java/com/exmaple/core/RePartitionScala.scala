package com.exmaple.core

import com.exmaple.common.CommSparkContextSca

object RePartitionScala {

  def main(args: Array[String]): Unit = {
    val sc = CommSparkContextSca.getsc()

    val list = Array("henry", "chery", "ben", "leo", "lili")

    val rdd = sc.parallelize(list, 2)

    val mapIndexValues = rdd.mapPartitionsWithIndex((index, x) => {
      var list = List[String]()
      while (x.hasNext) {
        val userName = x.next();
        list.::=((index + 1) + ":" + userName)
      }
      list.iterator
    })

    val coalesceValues = mapIndexValues.repartition(3)

    val mapIndexValues2 = coalesceValues.mapPartitionsWithIndex((index, x) => {
      var list = List[String]()
      while (x.hasNext) {
        val userName = x.next()
        list.::=((index + 1) + ":" + userName)
      }
      list.iterator
    })

    for (elem <- mapIndexValues2.collect()) {
      System.out.println(elem)
    }

  }
}
