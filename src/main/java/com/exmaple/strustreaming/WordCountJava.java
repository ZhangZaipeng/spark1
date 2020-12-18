package com.exmaple.strustreaming;

import com.exmaple.common.CommSparkSessionJava;
import java.util.Arrays;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

public class WordCountJava {
    /**
     * input data :
     * spark java hive java     spark ,1
     *                          java ,2
     *                          hive ,1
     *
     * java spark                spark ,2                 spark,2
     *           complete ->     java , 3       update -> java ,3
     *                           hive ,1
     *
     *
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
    SparkSession spark = CommSparkSessionJava.getSparkSession();

    Dataset<Row> lines = spark
        .readStream()
        .format("socket")
        .option("host", "192.172.1.40")
        .option("port", 9999)
        .load();

    Dataset<String> words = lines.as(Encoders.STRING())
        .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

    Dataset<Row> wordCount = words.groupBy("value").count();

    StreamingQuery query = wordCount.writeStream()
        .outputMode("complete")
        .format("console")
        .start();

    query.awaitTermination();
  }
}
