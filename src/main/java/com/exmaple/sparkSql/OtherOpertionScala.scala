package com.exmaple.sparkSql

import com.exmaple.common.{Comm, CommSparkSessionScala}

object OtherOpertionScala {

  case class Dept(id: String, deptName: String)

  case class Employee(deptId: String, name1: String, salary: Long)

  def main(args: Array[String]): Unit = {

    val spark = CommSparkSessionScala.getSparkSession()

    val employeesPath = Comm.fileDirPath + "employees.json"
    val deptPath = Comm.fileDirPath + "dept.json"

    import spark.implicits._
    val employeeDS = spark.read.json(employeesPath).as[Employee]

    val deptDS = spark.read.json(deptPath).as[Dept]

    val joinDS = employeeDS.join(deptDS, $"deptId" === $"id");

    import org.apache.spark.sql.functions._


    /**
     * 日期函数 ： current_date  current_timestamp
     * 数学函数：round
     * 随机函数：rand
     * 字符串函数： concat
     */

    joinDS.select(
      current_date(),
      current_timestamp(),
      rand(),
      round($"salary", 2),
      concat($"name1", $"salary")).show()

  }

}
