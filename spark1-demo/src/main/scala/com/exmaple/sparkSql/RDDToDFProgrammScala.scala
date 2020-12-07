package com.exmaple.sparkSql

import com.exmaple.common.{Comm, CommSparkSessionScala}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object RDDToDFProgrammScala {

  def main(args: Array[String]): Unit = {

    val spark = CommSparkSessionScala.getSparkSession();

    val scheme = StructType(Array(
      StructField("name", StringType, true),
      StructField("age", LongType, true)
    ))

    val path = Comm.fileDirPath + "people.txt"
    val rdd = spark.sparkContext.textFile(path).map(line => line.split(",")).map(x => {
      Row(x(0), x(1).trim.toLong)
    })

    val personDF = spark.createDataFrame(rdd, scheme)

    personDF.show()

    personDF.createOrReplaceTempView("person");

    val resultDF = spark.sql("select * from person a where a.age > 25")
    resultDF.show()

    for (elem <- resultDF.collect()) {
      System.out.println(elem.get(0) + " : " + elem.get(1))
    }

  }

}
