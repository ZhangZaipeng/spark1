package com.exmaple.project6;

import com.exmaple.common.CommSparkContext;
import com.exmaple.common.CommSparkSessionJava;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class TopHotProduct {
  /**
   * 案例： 每隔10秒，统计最近60秒每隔种类的每个商品的点击次数，然后统计出每个种类top3的热门商品
   * 数据的输入格式：
   * ------------------------
   * 姓名     商品分类     商品名称
   * leo     bigdata     hadooop
   * leo     bigdata     spark
   * leo     language    java
   * leo     language    python
   * cherry  bigdata     storm
   * --------------------------------
   * 第一步：
   * <bigdata_hadoop,1>
   * <bigdata_spark,1>
   *
   * 第二步：reduceByKeyAndWindow
   * <bigdata_hadoop,5>
   * <bigdata_spark,1>
   * <bigdata_storm,3>
   *
   * 第三步:foreachRDD  -> DataFrame  ->  registerTempView  -> sql)
   *
   * @param args
   */
  public static void main(String[] args) throws Exception {
    JavaStreamingContext jssc = CommSparkContext.getJssc();

    JavaReceiverInputDStream<String> inputDStream =
        jssc.socketTextStream("bigdata-pro-m01.kfk.com", 9999);

    JavaPairDStream<String, Integer> pairDStream = inputDStream.mapToPair(x -> {
      String[] lines = x.split(" ");
      return new Tuple2<>(lines[1] + "_" + lines[2], 1);
    });

    JavaPairDStream<String, Integer> windowPairStream =
        pairDStream.reduceByKeyAndWindow((x, y) -> x + y,
            Durations.seconds(60), Durations.seconds(10));

    windowPairStream.foreachRDD(x -> {
      JavaRDD countRowRDD = x.map(tuple -> {
        String category = tuple._1.split("_")[0];
        String product = tuple._1.split("_")[1];
        Integer count = tuple._2;
        return RowFactory.create(category, product, count);
      });

      List<StructField> structFields = new ArrayList<StructField>();
      structFields.add(DataTypes.createStructField("category", DataTypes.StringType, true));
      structFields.add(DataTypes.createStructField("product", DataTypes.StringType, true));
      structFields.add(DataTypes.createStructField("product_count", DataTypes.IntegerType, true));

      StructType structType = DataTypes.createStructType(structFields);

      SparkSession sparkSession = CommSparkSessionJava.getSparkSession();

      Dataset<Row> df = sparkSession.createDataFrame(countRowRDD, structType);

      df.createOrReplaceTempView("product_click");

      /**
       * 统计每个种类下点击次数排名前3的商品
       */

      sparkSession.sql("" +
          "select category,product,product_count,rank from" +
          " (select category,product,product_count,row_number() OVER (PARTITION BY category,product order by product_count desc) rank "
          +
          " from product_click ) tempUser where rank <=3" +
          "").show();
    });

    jssc.start();

    jssc.awaitTermination();

    jssc.close();

  }
}
