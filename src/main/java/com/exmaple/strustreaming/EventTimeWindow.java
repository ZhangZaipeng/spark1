package com.exmaple.strustreaming;

import com.exmaple.common.CommSparkSessionJava;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import scala.Tuple2;

/**
 * 数据源： spark hadoop
 */
public class EventTimeWindow {

  public static void main(String[] args) throws Exception {
    SparkSession spark = CommSparkSessionJava.getSparkSession();

    Dataset<Row> df_line = spark
        .readStream()
        .format("socket")
        .option("host", "192.172.1.40")
        .option("port", 9999)
        .option("includeTimestamp", true)
        .load();

    /**
     * 输入数据：spark storm hive
     * 接受到数据模型：
     * 第一步：words -> spark storm hive,  timestamp -> 2019-09-09 12:12:12
     *        eg: tuple -> (spark storm hive , 2019-09-09 12:12:12)
     *
     * 第二步：split()操作
     *        对tuple中的key进行split操作
     *
     * 第三步：flatMap()操作
     * 返回类型 -> list(tuple(spark,2019-09-09 12:12:12),
     *               tuple(storm,2019-09-09 12:12:12),
     *               tuple(hive,2019-09-09 12:12:12))
     */

    Dataset<Row> words = df_line.as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
        .flatMap((FlatMapFunction<Tuple2<String, Timestamp>, Tuple2<String, Timestamp>>) t -> {
          List<Tuple2<String, Timestamp>> result = new ArrayList<>();
          for (String word : t._1.split(" ")) {
            result.add(new Tuple2<>(word, t._2));
          }
          return result.iterator();
        }, Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
        .toDF("word", "wordtime");

    Dataset<Row> windowedCounts = words.groupBy(
        functions.window(words.col("wordtime"),
            "10 minutes",
            "5 minutes"),
        words.col("word")
    ).count();

    StreamingQuery query = windowedCounts.writeStream()
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .start();

    query.awaitTermination();

  }
}
