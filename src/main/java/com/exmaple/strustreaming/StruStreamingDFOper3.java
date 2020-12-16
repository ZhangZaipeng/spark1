package com.exmaple.strustreaming;

import com.exmaple.common.CommSparkSessionJava;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class StruStreamingDFOper3 {

  public static void main(String[] args) throws Exception {

    SparkSession spark = CommSparkSessionJava.getSparkSession();

    Dataset<String> df_line = spark
        .readStream()
        .format("socket")
        .option("host", "bigdata-pro-m01.kfk.com")
        .option("port", 9999)
        .load().as(Encoders.STRING());

    List<StructField> fields = new ArrayList<StructField>();
    StructField structField_device = DataTypes
        .createStructField("device", DataTypes.StringType, true);
    StructField structField_deviceType = DataTypes
        .createStructField("deviceType", DataTypes.StringType, true);
    StructField structField_signal = DataTypes
        .createStructField("signal", DataTypes.DoubleType, true);
    StructField structField_deviceTime = DataTypes
        .createStructField("deviceTime", DataTypes.DateType, true);

    fields.add(structField_device);
    fields.add(structField_deviceType);
    fields.add(structField_signal);
    fields.add(structField_deviceTime);

    StructType scheme = DataTypes.createStructType(fields);

    Dataset<Row> df_row = df_line.map((MapFunction<String, Row>) x -> {
      String[] lines = x.split(",");
      return RowFactory
          .create(lines[0], lines[1], Double.valueOf(lines[2]), Date.valueOf(lines[3]));

    }, RowEncoder.apply(scheme));
    Dataset<DeviceData> df_device = df_row.as(ExpressionEncoder.javaBean(DeviceData.class));
    df_device.createOrReplaceTempView("device");
    Dataset<Row> df_final = spark.sql("select * from device");

    StreamingQuery query = df_final.writeStream()
        .outputMode("append")
        .format("console")
        .start();
    query.awaitTermination();

  }
}
