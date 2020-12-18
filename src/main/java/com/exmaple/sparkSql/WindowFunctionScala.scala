package com.exmaple.sparkSql

import com.exmaple.common.{Comm, CommSparkSessionScala}
import org.apache.spark.sql.SparkSession

object WindowFunctionScala {

  case class USER(name: String, deptName: String, salary: Integer);

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local").getOrCreate();

    import spark.implicits._

    val caseClassDS = Seq(
      USER("cherry", "dept1", 2000),
      USER("lili", "dept2", 2100),
      USER("anny", "dept1", 2200),
      USER("zhang", "dept2", 2400),
      USER("wang", "dept1", 2500)).toDS()

    caseClassDS.createOrReplaceTempView("user")

    spark.sql(
      "select name,deptName,salary,rank from" +
        " (select name, deptName, salary, row_number() OVER (PARTITION BY deptName order by salary desc) rank " +
        " from user ) tempUser where rank <=10" +
        "").show()
  }
}
