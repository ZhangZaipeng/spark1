package com.exmaple.core

import com.exmaple.common.CommSparkContextSca

/**
 * 计算两个RDD之间的笛卡尔积（即第一个RDD的每个项与第二个RDD的每个项连接）并将它们作为新的RDD返回
 */
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
