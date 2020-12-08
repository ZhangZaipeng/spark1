package com.exmaple.sparkSql

import com.exmaple.common.{Comm, CommSparkSessionScala}

object UntypeOpertionScala {

  case class Dept(id: String, deptName: String)

  case class Employee(deptId: String, name1: String, salary: Long)


  def main(args: Array[String]): Unit = {

    val spark = CommSparkSessionScala.getSparkSession()

    val employeesPath = Comm.fileDirPath + "employees.json"
    val deptPath = Comm.fileDirPath + "dept.json"

    import spark.implicits._

    val employeeDS = spark.read.json(employeesPath).as[Employee]

    val deptDS = spark.read.json(deptPath).as[Dept]

    val joinDS = employeeDS.join(deptDS, $"deptId" === $"id")

    //    joinDS.groupBy("deptId","deptName").sum("salary")
    //      .select("deptId","deptName","sum(salary)").orderBy($"sum(salary)".desc).show()

    joinDS.toDF().createOrReplaceTempView("deptSalary")

    val resultDF = spark.sql("select a.deptId,a.deptName,sum(a.salary) as salary " +
      "from deptSalary a where 1=1 group by a.deptId,a.deptName order by salary desc")

    resultDF.show()

  }

}
