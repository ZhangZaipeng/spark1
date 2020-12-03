package com.exmaple.project1

import com.exmaple.common.CommSparkContextSca
import org.apache.spark.SparkContext

/**
 * @Description ：
 * @Tauthor ZhangZaipeng
 * @Tdata 2020/12/2   14:51
 */
object LogAppScala {

  case class LogInfo(timeStamp: Long, upTraffic: Long, downTraffic: Long)

  def main(args: Array[String]): Unit = {
    val sc = CommSparkContextSca.getsc()

    val rddLine = sc.textFile("E:/docs/hadoop/spark1/spark1-demo/src/main/scala/com/exmaple/p1/access.log")

    val logInfoRDD = rddLine.map(x => {
      val array = x.split("\t")
      (array(1), LogInfo(array(0).toLong, array(2).toLong, array(3).toLong))
    })

    val aggreByDeviceIdRDD = logInfoRDD.groupByKey()

    val mergeRDD = aggreByDeviceIdRDD.map(x => {
      var timeStamp: Long = 0
      var upTraffic: Long = 0
      var downTraffic: Long = 0
      x._2.foreach(
        y => {
          timeStamp = y.timeStamp + timeStamp
          upTraffic = y.upTraffic + upTraffic
          downTraffic = y.downTraffic + downTraffic
        }
      )
      (x._1, new LogSort(timeStamp, upTraffic, downTraffic))
    })

    val sortRDD = mergeRDD.map(x => (x._2, x._1))

    sortRDD.sortByKey(false).take(10).foreach(x => {
      System.out.println(x._2 + " : " + x._1.getUpTraffic + " : " + x._1.getDownTraffic)
    })

  }
}
