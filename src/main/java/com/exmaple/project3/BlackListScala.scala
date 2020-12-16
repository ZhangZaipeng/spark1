package com.exmaple.project3

import com.exmaple.common.CommSparkContextSca

object BlackListScala {

  def main(args: Array[String]): Unit = {

    val jssc = CommSparkContextSca.getJssc()

    val blackList = Array(("ben", true), ("leo", true))

    val blackListRDD = jssc.sparkContext.parallelize(blackList)

    val userDStream = jssc.socketTextStream("bigdata-pro-m01.kfk.com", 9999)
    //源数据：2019-09-09 李四      ->  转换后数据： <“张三”, 2019-09-09 张三>
    val pair = userDStream.map(line => (line.split(" ")(1), line))

    val valiStream = pair.transform(tuple => {
      val leftJoinRDD = tuple.leftOuterJoin(blackListRDD);
      val filterRDD = leftJoinRDD.filter(rdd => {
        if (rdd._2._2.getOrElse(0) == true) false else true
      })
      val valiRDD = filterRDD.map(tuple => (tuple._1 + " : " + tuple._2._1 + ":" + tuple._2._2))
      valiRDD
    })

    valiStream.print()

    jssc.start()

    jssc.awaitTermination()

  }

}
