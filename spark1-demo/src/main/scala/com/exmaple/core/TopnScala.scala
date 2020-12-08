package com.exmaple.core

import com.exmaple.common.CommSparkContextSca

object TopnScala {

  def main(args: Array[String]): Unit = {
    val sc = CommSparkContextSca.getsc();
    val list = Array(23, 12, 56, 44, 23, 99, 13, 57);
    val rdd = sc.parallelize(list)

    val beginSort = rdd.map(x => (x, x))
    val sortValue = beginSort.sortByKey(false)

    val beginTopn = sortValue.map(x => x._2)
    val topn = beginTopn.take(3);

    for (elem <- topn) {
      System.out.println(elem)
    }
  }
}
