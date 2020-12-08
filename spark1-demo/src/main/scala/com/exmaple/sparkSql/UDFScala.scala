package com.exmaple.sparkSql

import com.exmaple.common.{Comm, CommSparkSessionScala}

object UDFScala {

  def main(args: Array[String]): Unit = {

    val spark = CommSparkSessionScala.getSparkSession()

    val userPath = Comm.fileDirPath + "user.json"

    spark.read.json(userPath).createOrReplaceTempView("user")

    spark.udf.register("strUpper", (str: String) => str.toUpperCase)

    spark.sql("select deptName,strUpper(name) from user").show()

  }

}
