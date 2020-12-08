package com.exmaple.sparkSql

import com.exmaple.common.{Comm, CommSparkSessionScala}
import org.apache.spark.sql.SparkSession

object AggOpertionScala {

  def main(args: Array[String]): Unit = {

    val spark = CommSparkSessionScala.getSparkSession()

    agg2(spark)

  }

  case class Dept(id: String, deptName: String)

  case class Employee(deptId: String, name1: String, salary: Long)

  /**
   * avg
   * sum
   * max
   * min
   * count
   *
   * @param spark
   */
  def agg1(spark: SparkSession): Unit = {
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val employeesPath = Comm.fileDirPath + "employees.json"
    val deptPath = Comm.fileDirPath + "dept.json"
    val employeeDS = spark.read.json(employeesPath).as[Employee]
    val deptDS = spark.read.json(deptPath).as[Dept];
    val joinDS = employeeDS.join(deptDS, $"deptId" === $"id");

    joinDS.groupBy("deptId", "deptName")
      .agg(avg("salary"), sum("salary"), max("salary"), min("salary"), count("name1")).show()

    joinDS.groupBy("deptId", "deptName")
      .agg("salary" -> "avg", "salary" -> "sum", "name1" -> "count").show()
  }


  /**
   * collect_list （行转列）将一个分组内指定字段的值收集在一起，不会去重
   * collect_set  （行转列）意思同上，唯一的区别就是它会去重
   *
   * @param spark
   */
  def agg2(spark: SparkSession): Unit = {

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val employeesPath = Comm.fileDirPath + "employees.json"
    val deptPath = Comm.fileDirPath + "dept.json"
    val employeeDS = spark.read.json(employeesPath).as[Employee]
    val deptDS = spark.read.json(deptPath).as[Dept]
    val joinDS = employeeDS.join(deptDS, $"deptId" === $"id")

    joinDS.groupBy("deptId", "deptName").agg(collect_list("name1")).show()

  }
}
