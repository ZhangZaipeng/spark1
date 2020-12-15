package com.exmaple.streaming

import com.exmaple.common.CommSparkContextSca


object StreamingUpdateByKeyScala {

  def main(args: Array[String]): Unit = {
    val jssc = CommSparkContextSca.getJssc()

    jssc.checkpoint("hdfs://bigdata-pro-m01.kfk.com:9000/user/kfk/datas/sparkCheckpoint")

    val lines = jssc.socketTextStream("bigdata-pro-m01.kfk.com", 9999)

    val wordCount = lines.flatMap(x => x.split(" ")).map(x => (x, 1))
      .updateStateByKey((values: Seq[Int], state: Option[Int]) => {
        var newValue = state.getOrElse(0)
        for (value <- values) {
          newValue += value
        }
        Option(newValue)
      });

    wordCount.print()

    jssc.start()

    jssc.awaitTermination()

  }

}
