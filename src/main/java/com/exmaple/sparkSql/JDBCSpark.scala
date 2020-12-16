package com.exmaple.sparkSql

import java.util.Properties

import com.exmaple.common.CommSparkSessionScala
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import spire.math.UInt

object JDBCSpark {

  case class StuPerson(name:String,age:Int)

  def main(args: Array[String]): Unit = {

    val spark = CommSparkSessionScala.getSparkSession();
    val df = getData1(spark)
    writeData1(spark, df)

  }

  def getData(spark: SparkSession): Dataset[Row] = {

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://192.172.1.40/test")
      .option("dbtable", "stu_person")
      .option("user", "admin")
      .option("password", "WV0Djimi")
      .load()

    val df = jdbcDF.select("id","name","age")

/*    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "123456")
    val jdbcDF2 = spark.read
      .jdbc("jdbc:mysql://bigdata-pro-m03.kfk.com/spark", "stu_person", connectionProperties)
    val df = jdbcDF2.select("id", "deptid", "name", "salary")*/
    df.show()
    df
  }

  def getData1(spark: SparkSession): Dataset[StuPerson] = {

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://192.172.1.40/test")
      .option("dbtable", "stu_person")
      .option("user", "admin")
      .option("password", "WV0Djimi")
      .load()

    val df = jdbcDF.select("id","name","age")

    /*    val connectionProperties = new Properties()
        connectionProperties.put("user", "root")
        connectionProperties.put("password", "123456")
        val jdbcDF2 = spark.read
          .jdbc("jdbc:mysql://bigdata-pro-m03.kfk.com/spark", "stu_person", connectionProperties)
        val df = jdbcDF2.select("id", "deptid", "name", "salary")*/
    df.show()

    import spark.implicits._

    val personDf = df.map(x => {
      StuPerson(x.getString(1),x.getInt(2))
    })
    personDf.show()
    return personDf
  }

  def writeData1(spark: SparkSession, jdbcDF: Dataset[StuPerson]): Unit = {

    jdbcDF.write
      .format("jdbc")
      .option("url", "jdbc:mysql://192.172.1.40/test")
      .option("dbtable", "stu_person")
      .option("user", "admin")
      .option("password", "WV0Djimi")
      .mode("append")
      .save()

  }



  def writeData(spark: SparkSession, jdbcDF: Dataset[Row]): Unit = {

    jdbcDF.write
      .format("jdbc")
      .option("url", "jdbc:mysql://192.172.1.40/test")
      .option("dbtable", "stu_person")
      .option("user", "admin")
      .option("password", "WV0Djimi")
      .save()

  }


}
