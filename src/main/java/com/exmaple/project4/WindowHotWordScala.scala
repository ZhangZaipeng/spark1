package com.exmaple.project4

import com.exmaple.common.CommSparkContextSca
import org.apache.spark.streaming.Seconds

/**
 * window operations：滑动窗口操作
 * window() 基于窗口得到原始的 DStream
 * countByWindow() 滑动窗口数据量中的元素个数
 * reduceByWindow()
 * reduceByKeyAndWindow()
 * countByValueAndWindow()
 */
object WindowHotWordScala {


  def main(args: Array[String]): Unit = {

    val jssc = CommSparkContextSca.getJssc();

    val inputStream = jssc.socketTextStream("192.172.1.40", 9999);

    val pairStream = inputStream.map(x => (x, 1))

    val windowDStream = pairStream.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(60), Seconds(10))

    val finalDStream = windowDStream.transform(x => {
      val sortRDD = x.map(x => (x._2, x._1)).sortByKey().map(x => (x._2, x._1))

      val list = sortRDD.take(3)
      jssc.sparkContext.parallelize(list)
    })

    finalDStream.print()

    jssc.start()
    jssc.awaitTermination()
  }

}
