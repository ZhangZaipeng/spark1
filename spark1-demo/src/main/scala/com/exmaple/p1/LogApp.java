package com.exmaple.p1;

import com.exmaple.common.CommSparkContext;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class LogApp {

  public static void main(String[] args) {
    JavaSparkContext sc = CommSparkContext.getsc();

    JavaRDD<String> rdd = sc
        .textFile("E:/docs/hadoop/spark1/spark1-demo/src/main/scala/com/exmaple/p1/access.log");

    JavaPairRDD<String, LogInfo> mapPairRDD = mapToPairRDD(rdd);
    JavaPairRDD<String, LogInfo> aggreRDD = aggreByDeviceID(mapPairRDD);
    JavaPairRDD<LogSort, String> mapPairsortRDD = mapToPairSortRDD(aggreRDD);

    JavaPairRDD<LogSort, String> sortRDD = mapPairsortRDD.sortByKey(false);

    List<Tuple2<LogSort, String>> list = sortRDD.take(10);

    for (Tuple2<LogSort, String> logSortStringTuple2 : list) {
      System.out.println(logSortStringTuple2._2 + " : "
          + logSortStringTuple2._1.getUpTraffic() +
          " : " + logSortStringTuple2._1.getDownTraffic());
    }

    //<diviceID  , LogInfo(upTraffic,downTraffic)>
  }

  /**
   * rdd 映射成 <key,value>  <deviceID,LogInfo>
   */
  private static JavaPairRDD<String, LogInfo> mapToPairRDD(JavaRDD<String> rdd) {
    return rdd.mapToPair(new PairFunction<String, String, LogInfo>() {
      @Override
      public Tuple2<String, LogInfo> call(String line) throws Exception {
        long timeStamp = Long.valueOf(line.split("\t")[0]);
        String deviceID = line.split("\t")[1];
        long upTraffic = Long.valueOf(line.split("\t")[2]);
        long downTraffic = Long.valueOf(line.split("\t")[3]);
        LogInfo logInfo = new LogInfo(timeStamp, upTraffic, downTraffic);
        return new Tuple2<String, LogInfo>(deviceID, logInfo);
      }
    });
  }

  /**
   * 根据deviceID进行聚合
   */
  private static JavaPairRDD<String, LogInfo> aggreByDeviceID(
      JavaPairRDD<String, LogInfo> mapPairRDD) {
    return mapPairRDD.reduceByKey(new Function2<LogInfo, LogInfo, LogInfo>() {
      @Override
      public LogInfo call(LogInfo logInfo1, LogInfo logInfo2) throws Exception {
        long timeStamp =
            (logInfo1.getTimeStamp() < logInfo2.getTimeStamp()) ? logInfo1.getTimeStamp() :
                logInfo2.getTimeStamp();
        long upTraffic = logInfo1.getUpTraffic() + logInfo2.getUpTraffic();
        long downTraffic = logInfo1.getDownTraffic() + logInfo2.getDownTraffic();
        LogInfo logInfo = new LogInfo();
        logInfo.setTimeStamp(timeStamp);
        logInfo.setUpTraffic(upTraffic);
        logInfo.setDownTraffic(downTraffic);
        return logInfo;
      }
    });
  }

  private static JavaPairRDD<LogSort, String> mapToPairSortRDD(
      JavaPairRDD<String, LogInfo> aggreRDD) {
    return aggreRDD.mapToPair(new PairFunction<Tuple2<String, LogInfo>, LogSort, String>() {
      @Override
      public Tuple2<LogSort, String> call(Tuple2<String, LogInfo> stringLogInfoTuple2)
          throws Exception {
        String deviceID = stringLogInfoTuple2._1;
        long timeStamp = stringLogInfoTuple2._2.getTimeStamp();
        long upTraffic = stringLogInfoTuple2._2.getUpTraffic();
        long downTraffic = stringLogInfoTuple2._2.getDownTraffic();
        LogSort logSort = new LogSort(timeStamp, upTraffic, downTraffic);
        return new Tuple2<LogSort, String>(logSort, deviceID);
      }
    });
  }

  // JavaPairRDD<String,LogInfo>     JavaPairRDD<LogSort,String>
  // sortByKey()


}
