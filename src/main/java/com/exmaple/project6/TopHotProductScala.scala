package com.exmaple.project6

import com.exmaple.common.{CommSparkContextSca, CommSparkSessionScala}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.Seconds

object TopHotProductScala {

  def main(args: Array[String]): Unit = {
    val jssc = CommSparkContextSca.getJssc()
    val inputDStream = jssc.socketTextStream("bigdata-pro-m01.kfk.com", 9999)
    val pairDStream = inputDStream.map(x => (x.split(" ")(1) + "_" + x.split(" ")(2), 1))

    val windowDStream = pairDStream.reduceByKeyAndWindow((x: Int, y: Int) => (x + y), Seconds(60), Seconds(10))

    windowDStream.foreachRDD(x => {

      val rowRDD = x.map(tuple => {
        val category = tuple._1.split("_")(0)
        val product = tuple._1.split("_")(1)
        val count = tuple._2
        Row(category, product, count)
      })

      val structType = StructType(Array(
        StructField("category", StringType, true),
        StructField("product", StringType, true),
        StructField("product_count", IntegerType, true)
      ))

      val spark = CommSparkSessionScala.getSparkSession()

      val df = spark.createDataFrame(rowRDD, structType)

      df.createOrReplaceTempView("product_click")

      spark.sql("" + "select category,product,product_count,rank from"
        + " (select category,product,product_count,row_number() OVER (PARTITION BY category,product order by product_count desc) rank "
        + " from product_click ) tempUser where rank <=3" + "").show()
    })


    jssc.start()

    jssc.awaitTermination()
  }

}
