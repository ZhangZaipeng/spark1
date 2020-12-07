package com.exmaple.sparkSql

import com.exmaple.common.CommSparkSessionScala

/**
 * 持久化  ：  cache   persist
 * 创建临时视图 ： createTempView  createOrReplaceTempView
 * 获取执行计划 ： explain
 * 查看schema : printSchema
 * 写数据到外部的数据存储系统  :  write
 * ds 与 df之间的转化  as   toDF
 */
object BasicOpertionScala {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {

    val spark = CommSparkSessionScala.getSparkSession()
    val df = spark.read.json("file:///E:/hdfs/input/people.json")
    import spark.implicits._

    df.cache()

    df.show()

    df.printSchema()

//    df.select("name").write.save("path")

    df.createOrReplaceTempView("person");

    val resultDF = spark.sql("select * from person a where 1=1 and age is not null")

    resultDF.show()

    resultDF.printSchema();

    spark.sql("select * from person a where 1=1").explain();

    val personDS = df.as[Person]

    personDS.show()

    personDS.printSchema()

    val personDF = personDS.toDF()

    personDF.show()

  }

}
