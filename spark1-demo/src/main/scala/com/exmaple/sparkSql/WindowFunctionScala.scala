package com.exmaple.sparkSql

import com.exmaple.common.{Comm, CommSparkSessionScala}

object WindowFunctionScala {

  def main(args: Array[String]): Unit = {

    val spark = CommSparkSessionScala.getSparkSession()
    val userPath = Comm.fileDirPath + "user.json"

    spark.read.json(userPath).createOrReplaceTempView("user")

    spark.sql("" +
      "select deptName,name,salary,rank from" +
      " (select deptName,name,salary,row_number() OVER (PARTITION BY deptName order by salary desc) rank " +
      " from user ) tempUser where rank <=2" +
      "").show()
  }
}
