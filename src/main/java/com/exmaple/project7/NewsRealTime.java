package com.exmaple.project7;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import kfk.spark.common.CommSparkContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

/**
 *
 * 数据源： kafka
 * 需求：
 * 1.  pv
 * 2.  uv
 * 3.  跳出率
 * 4.  注册用户数
 * 5.  热门板块
 */
public class NewsRealTime {

    /**
     * input data:
     * 2019-03-09 1552120347477 9582 264 movie view
     * 2019-03-09 1552120347477 9659 783 tv-show view
     * @param args
     */
    public static void main(String[] args) throws Exception{
        JavaStreamingContext jssc = CommSparkContext.getJssc();

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "bigdata-pro-m01.kfk.com:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "streaming_kafka_1");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("spark");
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        JavaDStream<String> accessDStream = stream.map(x -> x.value());

        JavaDStream<String> filterDStream = accessDStream.filter(x -> {
            String[] lines = x.split(" ");
            String action  = lines[5] ;
            if("view".equals(action)){
                return true ;
            }else{
                return false ;
            }
        });

        //calculatePagePV(filterDStream) ;

        //calculatePageUV(filterDStream);
        //calculateRegistercount(accessDStream);
        //calculateUserJumpCount(filterDStream);
        calculateUserSectionPV(filterDStream);

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }

    /**
     * 求PV
     *
     * 2019-03-09 1552120347477 9582 264 movie view
     * <2019-03-09_264,1>
     * @param dStream
     */
    private static void calculatePagePV(JavaDStream<String> dStream){

        JavaPairDStream<String,Integer> pairDStream = dStream.mapToPair(x -> {
              String[] lines = x.split(" ");
              return new Tuple2<>(lines[0]+"_"+lines[3],    1);
        });
        JavaPairDStream<String,Integer> pvStream = pairDStream.reduceByKey((x , y) -> x + y);
        pvStream.print();
    }


    /**
     * input Data:
     * 2019-03-09 1552120347477 9582 264 movie view
     * 2019-03-09 1552120347477 9582 264 movie view
     * 2019-03-09 1552120347477 9583 264 movie view
     * 2019-03-09 1552120347477 9584 264 movie view
     * 2019-03-09 1552120347477 9585 264 movie view
     *
     * 第一步：rdd -> map
     * (2019-03-09,9582,264)
     * (2019-03-09,9582,264)
     * (2019-03-09,9583,264)
     * (2019-03-09,9584,264)
     * (2019-03-09,9585,264)
     *
     * 第二步:rdd -> distinct
     * (2019-03-09,9582,264)
     * (2019-03-09,9583,264)
     * (2019-03-09,9584,264)
     * (2019-03-09,9585,264)
     *
     * 第三步：mapToPair
     * (2019-03-09_264,1)
     * (2019-03-09_264,1)
     * (2019-03-09_264,1)
     * (2019-03-09_264,1)
     *
     * 第四步：reduceByKey
     * (2019-03-09_264,4)
     * @param dStream
     */
    private static void calculatePageUV(JavaDStream<String> dStream){

        JavaDStream<String> disBeginDStream = dStream.map(x -> {

            String[] lines = x.split(" ");
            return lines[0]+"_"+lines[2]+"_"+lines[3];
        });

       JavaPairDStream<String,Integer> uvDStream =
               disBeginDStream.transform(x -> x.distinct()).mapToPair(x -> {
                    String[] lines = x.split("_");
            return new Tuple2<>(lines[0]+"_"+lines[2] , 1);
        }).reduceByKey((x , y) -> x + y);
        uvDStream.print();

    }

    /**
     *
     * 2019-03-09 1552120347477 9582 null null register
     * 2019-03-09 1552120347477 9582 null null register
     * 获取当前的用户注册数
     * 过滤出action=register的数据就可以
     *
     *
     * 2019-03-09_register
     * 2019-03-09_register
     * 2019-03-10_register
     * 2019-03-10_register
     * @param dStream
     */
    private static void calculateRegistercount(JavaDStream<String> dStream){
        JavaDStream<String> filterDStream = dStream.filter(x -> {
             String[] lines = x.split(" ");
             String action = lines[5];
             if("register".equals(action)){
                 return true;
             }else{
                 return false ;
             }
        });

        JavaPairDStream<String,Integer> countDStream = filterDStream.mapToPair(x -> {
             String[] lines = x.split(" ");
             return new Tuple2<>(lines[0]+"_"+lines[5] , 1);

        }).reduceByKey((x ,y) -> x + y);
        countDStream.print();
    }

    /**
     * 2019-03-09 1552120347477 9582 null null register
     *
     * 第一步：mapToPair
     * 2019-03-09_9582,1
     *
     * 第二步：reduceByKey
     * <2019-03-09_9582,2>
     *
     * 第三步:fileter
     * 过滤count  = 1 的数据
     *
     * 第四步：count
     *
     * @param dStream
     */
    private static void calculateUserJumpCount(JavaDStream<String> dStream){

        JavaDStream<Long> jumpDStream = dStream.mapToPair(x -> {
            String[] liens = x.split(" ");
            return  new Tuple2<>(liens[0]+"_"+liens[2],1);
        }).reduceByKey((x , y) -> x + y).filter(x -> {
            if(x._2 == 1){
                return true ;
            }else{
                return false ;
            }
        }).count();

        jumpDStream.print();

    }

    /**
     *2019-03-09 1552120347477 599 152 sport view
     * 第一步：mapToPair
     * (2019-03-09_sport,1)
     *
     * 第二步：reduceByKey
     * @param dStream
     */
    private static void calculateUserSectionPV(JavaDStream<String> dStream){
          JavaPairDStream<String,Integer> sectionDStream = dStream.mapToPair(x -> {
              String[] lines = x.split(" ");
              return new Tuple2<>(lines[0]+"_"+lines[4] , 1);
          }).reduceByKey((x ,y ) -> x + y);

          sectionDStream.print();
    }

}
