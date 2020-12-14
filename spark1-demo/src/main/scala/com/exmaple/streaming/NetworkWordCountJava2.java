/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exmaple.streaming;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 *
 * Usage: JavaNetworkWordCount <hostname> <port>
 * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive
 * data.
 *
 * To run this on your local machine, you need to first run a Netcat server `$ nc -lk 9999` and then
 * run the example `$ bin/run-example org.apache.spark.examples.streaming.JavaNetworkWordCount
 * localhost 9999`
 */
public final class NetworkWordCountJava2 {

  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: JavaNetworkWordCount <hostname> <port>");
      System.exit(1);
    }

    StreamingExamples.setStreamingLogLevels();

    // Create the context with a 1 second batch size
    SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount");
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

    // Create a JavaReceiverInputDStream on target ip:port and count the
    // words in input stream of \n delimited text (eg. generated by 'nc')
    // Note that no duplication in storage level only for running locally.
    // Replication necessary in distributed scenario for fault tolerance.
    JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
        args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER);

    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String x) throws Exception {
        return Arrays.asList(SPACE.split(x)).iterator();
      }
    });

    JavaPairDStream<String, Integer> wordCounts =
        words.mapToPair(new PairFunction<String, String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(String s) throws Exception {
            return new Tuple2<>(s, 1);
          }
        });

    JavaPairDStream<String, Integer> reduceCounts = wordCounts.reduceByKey((i1, i2) -> i1 + i2);

    reduceCounts.print();

    ssc.start();
    ssc.awaitTermination();
  }
}
