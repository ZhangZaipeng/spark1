package com.exmaple.sparkSql

import org.apache.spark.sql.SparkSession

object SqlExample {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("test")
      .master("local").getOrCreate()


    import spark.implicits._
    val path = "file:///E:/hdfs/input/people.json"
    val df = spark.read.json(path)
    df.show()

    //    |-- age: long (nullable = true)
    //    |-- name: string (nullable = true)
    //df.select("name","age").show()
    //    df.select($"name",$"age" + 1).show()
    //    df.filter($"age" < 20).show()
    //    df.groupBy("age").count().show()

    //    df.createOrReplaceTempView("test");
    //
    //    spark.sql("select * from test").show()

    val caseClassDS = Seq(Person("cherry", 20)).toDS()
    caseClassDS.map(x => x.age + 1).show()
    caseClassDS.show()

    val commonDS = Seq(1, 2, 3).toDS()
    commonDS.map(x => x + 1).show()
    //commonDS.show()

    val jsonDS = spark.read.json(path).as[Person]
    jsonDS.show()

  }

}
