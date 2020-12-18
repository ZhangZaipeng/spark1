package com.exmaple.project5;

import com.exmaple.common.CommSparkContext;
import com.exmaple.common.ConnectionPool;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class ForeachPersistMySQL {

  public static void main(String[] args) throws Exception {
    JavaStreamingContext jssc = CommSparkContext.getJssc();
    jssc.checkpoint("hdfs://bigdata-pro-m01.kfk.com:9000/user/kfk/datas/sparkCheckpoint");
    JavaReceiverInputDStream<String> lines =
        jssc.socketTextStream("bigdata-pro-m01.kfk.com", 9999);
    JavaDStream<String> words = lines.flatMap(line ->
        Arrays.asList(line.split(" ")).iterator());

    JavaPairDStream<String, Integer> pair = words.mapToPair(word -> new Tuple2<>(word, 1));

    //通过spark来维护一份每个单词的全局统计次数
    JavaPairDStream<String, Integer> wordCount = pair.updateStateByKey((values, state) -> {
      Integer newValues = 0;
      if (state.isPresent()) {
        newValues = state.get();
      }

      for (Integer value : values) {
        newValues += value;
      }
      return Optional.of(newValues);
    });

    wordCount.foreachRDD(x -> x.foreachPartition(tuple -> {
      Tuple2<String, Integer> wordcount = null;
      Connection conn = ConnectionPool.getConnection();
      while (tuple.hasNext()) {
        wordcount = tuple.next();
        String sql = "insert into spark.wordcount(word,count) " +
            " values('" + wordcount._1 + "', " + wordcount._2 + ") ";

        Statement stm = conn.createStatement();
        stm.executeUpdate(sql);
      }
      ConnectionPool.returnConnection(conn);
    }));

    jssc.start();

    jssc.awaitTermination();
    jssc.close();

  }

}
