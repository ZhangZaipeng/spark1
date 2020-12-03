package com.exmaple.core

import com.exmaple.common.CommSparkContextSca

object CartesianScala {

  def main(args: Array[String]): Unit = {
    val sc = CommSparkContextSca.getsc()

    val list1 = Array("衣服-1", "衣服-2")
    val list2 = Array("裤子-1", "裤子-2")

    for (elem <- sc.parallelize(list1).cartesian(sc.parallelize(list2)).collect()) {
      System.out.println(elem)
    }
  }

}
