package com.exmaple.sparkSql

import com.exmaple.common.{Comm, CommSparkSessionScala}

object OpertionActionScala {

  def main(args: Array[String]): Unit = {

    val spark = CommSparkSessionScala.getSparkSession()
    val path = Comm.fileDirPath + "people.json"

    val df = spark.read.json(path)

    for (elem <- df.collect()) {
      System.out.println(elem)
    }

    System.out.println(df.first())

    for (elem <- df.take(2)) {
      System.out.println(elem)
    }
  }
}
