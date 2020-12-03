package com.exmaple.core

import com.exmaple.common.CommSparkContextSca

object MapPartitionsWithIndexScala {

  def main(args: Array[String]): Unit = {
    val sc = CommSparkContextSca.getsc();
    val list = Array("henry", "cherry", "leo", "ben")

    val rdd = sc.parallelize(list, 2);

    val indexValues = rdd.mapPartitionsWithIndex((_index, x) => {
      var list = List[String]();
      while (x.hasNext) {
        val userNameIndex = x.next() + ":" + (_index + 1)
        list.::=(userNameIndex)
      }
      list.iterator
    })

    indexValues.foreach(x => System.out.println(x))
  }
}
