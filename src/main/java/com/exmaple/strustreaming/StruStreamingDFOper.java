package com.exmaple.strustreaming;

import com.exmaple.common.CommSparkSessionJava;
import java.sql.Date;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;

public class StruStreamingDFOper {

  public static void main(String[] args) throws Exception {

    SparkSession spark = CommSparkSessionJava.getSparkSession();

    Dataset<String> df_line = spark
        .readStream()
        .format("socket")
        .option("host", "bigdata-pro-m01.kfk.com")
        .option("port", 9999)
        .load().as(Encoders.STRING());

    Dataset<DeviceData> df_device = df_line.map((MapFunction<String, DeviceData>) x -> {
      String[] lines = x.split(",");
      return new DeviceData(lines[0], lines[1], Double.valueOf(lines[2]), Date.valueOf(lines[3]));
    }, ExpressionEncoder.javaBean(DeviceData.class));

    Dataset<Row> df_final = df_device
        .select("device", "deviceType")
        //.where("signal > 10")
        .groupBy("deviceType")
        .count();

    StreamingQuery query = df_final.writeStream()
        .outputMode("update")
        .format("console")
        .start();

    query.awaitTermination();
  }
}
