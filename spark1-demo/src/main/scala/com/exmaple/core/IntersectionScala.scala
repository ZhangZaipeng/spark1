package com.exmaple.core

import com.exmaple.common.CommSparkContextSca

object IntersectionScala {
  def main(args: Array[String]): Unit = {
    val sc = CommSparkContextSca.getsc();
    val list1 = Array("cherry", "henry");
    val list2 = Array("leo", "henry", "lili");

    val rdd1 = for (elem <- sc.parallelize(list1).intersection(sc.parallelize(list2)).collect()) {
      System.out.println(elem)
    }
  }
}
