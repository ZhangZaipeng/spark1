package com.exmaple.sparkSql;

import com.exmaple.common.Comm;
import com.exmaple.common.CommSparkSessionJava;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LoadAndSaveJava {

  public static void main(String[] args) {
    SparkSession spark = CommSparkSessionJava.getSparkSession();
//        Dataset<Row> userDF = spark.read().load(Comm.fileDirPath + "users.parquet");
//        userDF.select("name", "favorite_color", "favorite_numbers")
//                .write().save(Comm.saveFileDirPath + "test.paruet");

    Dataset<Row> userDF = spark.read().format("parquet").load(Comm.fileDirPath + "users.parquet");

    userDF.select("name", "favorite_color").write().format("json")
        .save(Comm.saveFileDirPath + "test.json");

  }
}
