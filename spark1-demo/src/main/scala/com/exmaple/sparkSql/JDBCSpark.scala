package com.exmaple.sparkSql

import java.util.Properties

import com.exmaple.common.CommSparkSessionScala
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object JDBCSpark {

  def main(args: Array[String]): Unit = {

    val spark = CommSparkSessionScala.getSparkSession();
    val df = getData(spark)
    writeData(spark, df)

  }

  def getData(spark: SparkSession): Dataset[Row] = {

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://bigdata-pro-m03.kfk.com/spark")
      .option("dbtable", "stu_person")
      .option("user", "root")
      .option("password", "123456")
      .load()

    //jdbcDF.select("id","deptid","name","salary").show()

    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "123456")
    val jdbcDF2 = spark.read
      .jdbc("jdbc:mysql://bigdata-pro-m03.kfk.com/spark", "stu_person", connectionProperties)
    val df = jdbcDF2.select("id", "deptid", "name", "salary")
    df
  }

  def writeData(spark: SparkSession, jdbcDF: Dataset[Row]): Unit = {

    jdbcDF.write
      .format("jdbc")
      .option("url", "jdbc:mysql://bigdata-pro-m03.kfk.com/spark")
      .option("dbtable", "stu_person_info")
      .option("user", "root")
      .option("password", "123456")
      .save()

  }


}
