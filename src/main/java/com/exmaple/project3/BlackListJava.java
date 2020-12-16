package com.exmaple.project3;

import com.exmaple.common.CommSparkContext;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;


public class BlackListJava {

  public static void main(String[] args) throws Exception {
    JavaStreamingContext jssc = CommSparkContext.getJssc();

    /**
     * 模拟创建一个黑名单
     * 数据格式
     *  <String,Boolean>  ->   <"ben",true>
     */
    List<Tuple2<String, Boolean>> blackList = new ArrayList<Tuple2<String, Boolean>>();
    blackList.add(new Tuple2<>("ben", true));
    blackList.add(new Tuple2<>("leo", true));

    JavaPairRDD<String, Boolean> blackListRdd = jssc.sparkContext().parallelizePairs(blackList);

    JavaReceiverInputDStream<String> userDStream =
        jssc.socketTextStream("192.172.1.40", 9999);

    /**
     * 源数据：2019-09-09 李四      ->  转换后数据： <“张三”, 2019-09-09 张三>
     */
    JavaPairDStream<String, String> userPaire = userDStream.mapToPair(line -> {
          return new Tuple2<>(line.split(" ")[1], line);
        });

    /**
     * leftOuterJoin   <“张三”, 2019-09-09 张三>  <“张三”,true >
     *
     <”张三”， <张三 2019-09-09，true> >
     */
    JavaDStream<String> validDStream = userPaire.transform(pair -> {

      JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinRdd =
          pair.leftOuterJoin(blackListRdd);

      JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> fileterJoinRDD =
          joinRdd.filter(tuple -> {
            if (tuple._2._2.isPresent() && tuple._2._2.get()) {
              return false;
            }
            return true;
          });

      JavaRDD<String> valiRDD = fileterJoinRDD.map(tuple -> {
        return tuple._1 + " : " + tuple._2._1 + " : " + tuple._2._2;
      });

      return valiRDD;
    });

    validDStream.print();

    jssc.start();

    jssc.awaitTermination();

  }

}
