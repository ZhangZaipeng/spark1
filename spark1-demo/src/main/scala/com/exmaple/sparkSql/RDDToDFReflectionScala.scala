package com.exmaple.sparkSql

import com.exmaple.common.{Comm, CommSparkSessionScala}

object RDDToDFReflectionScala {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {

    val spark = CommSparkSessionScala.getSparkSession()
    val path = Comm.fileDirPath + "people.txt"


    import spark.implicits._

    val personDF = spark.sparkContext.textFile(path).map(line => line.split(",")).map(x => {
      Person(x(0), x(1).trim.toLong)
    }).toDF()

    personDF.createOrReplaceTempView("person")

    val resultDF = spark.sql("select * from person a where a.age > 20");

    val resultRDD = resultDF.rdd.map(row => {
      val name = row.getAs[String]("name");
      val age = row.getAs[Long]("age");

      Person(name, age)
    });

    for (elem <- resultRDD.collect()) {
      System.out.println(elem.name + " : " + elem.age)
    }

  }

}
