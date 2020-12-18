package com.exmaple.strustreaming;

import com.exmaple.common.CommSparkSessionJava;
import java.sql.Timestamp;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;

/**
 * 数据源：2019-03-29 16:40:00,hadoop 2019-03-29 16:40:10,storm
 */
public class EventTimeWindow2 {

  public static void main(String[] args) throws Exception {
    SparkSession spark = CommSparkSessionJava.getSparkSession();

    Dataset<String> df_line = spark
        .readStream()
        .format("socket")
        .option("host", "192.172.1.40")
        .option("port", 9999)
        .load().as(Encoders.STRING());

    Dataset<EventData> df_eventdata = df_line.map((MapFunction<String, EventData>) x -> {
      String[] lines = x.split(",");
      return new EventData(Timestamp.valueOf(lines[0]), lines[1]);
    }, ExpressionEncoder.javaBean(EventData.class));

    Dataset<Row> windowedCounts = df_eventdata.groupBy(
        functions.window(df_eventdata.col("wordtime"),
            "10 minutes",
            "5 minutes"),
        df_eventdata.col("word")
    ).count();

    StreamingQuery query = windowedCounts.writeStream()
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", "hdfs://hadoop1:8020/spark/checkpoint/event_time2")
        .start();
    query.awaitTermination();

  }
}
