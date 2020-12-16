package com.exmaple.sparkSql

import com.exmaple.common.{Comm, CommSparkSessionScala}
import org.apache.spark.sql.{SaveMode, SparkSession}

object ParquetScala {

  def main(args: Array[String]): Unit = {

    val spark = CommSparkSessionScala.getSparkSession();

    // basicLoad(spark);
    // partition(spark);
//    mege(spark)
        mege2(spark);

  }

  def basicLoad(spark: SparkSession): Unit = {
    val jsonPath = Comm.fileDirPath + "people.json"
    val parquetPath = Comm.fileDirPath + "people.parquet"
    val peopleDF = spark.read.json(jsonPath)
    //peopleDF.write.parquet(Comm.fileDirPath + "people.parquet")

    import spark.implicits._;

    val parquetDF = spark.read.parquet(parquetPath).createOrReplaceTempView("people");
    spark.sql("select * from people").map(row => "name : " + row.getAs("name")).show()
  }

  def partition(spark: SparkSession): Unit = {
    val partitionPath = Comm.fileDirPath + "people"
    //import   spark.implicits._;
    spark.read.parquet(partitionPath).show()
  }

  def mege(spark: SparkSession): Unit = {
    import spark.implicits._;

    val squareDF = spark.sparkContext.makeRDD(1 to 5).map(x => (x, x * x)).toDF("value", "square")
    squareDF.write.parquet(Comm.fileDirPath + "test_table/key=1")
    squareDF.show()

    val cubeDF = spark.sparkContext.makeRDD(6 to 10).map(x => (x, x * x * x)).toDF("value", "cube");
    cubeDF.write.parquet(Comm.fileDirPath + "test_table/key=2");
    cubeDF.show()

    val megeDF = spark.read.option("mergeSchema", true).parquet(Comm.fileDirPath + "test_table");
    megeDF.printSchema()

  }

  def mege2(spark: SparkSession): Unit = {
    import spark.implicits._

    val array1 = Array(("cherry", 22), ("leo", 33))
    val arrayDF1 = spark.sparkContext.makeRDD(array1.toSeq).toDF("name", "age")
      .write.format("parquet").mode(SaveMode.Append).save(Comm.fileDirPath + "test_table2");

    val array2 = Array(("cherry", "dept-1"), ("leo", "dept-2"));
    val arrayDF2 = spark.sparkContext.makeRDD(array2.toSeq).toDF("name", "dept")
      .write.format("parquet").mode(SaveMode.Append).save(Comm.fileDirPath + "test_table2");

    val megeDF = spark.read.option("mergeSchema", true).parquet(Comm.fileDirPath + "test_table2");
    megeDF.printSchema()

  }

}
