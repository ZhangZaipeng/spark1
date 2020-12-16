package com.exmaple.core;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

/**
 * Spark为此提供了两种共享变量，一种是Broadcast Variable（广播变量, 只读），另一种是Accumulator（累加变量）。
 * 1.
 */
public class VarJava {

    public static JavaSparkContext getsc(){
        SparkConf sparkConf = new SparkConf().setAppName("parallelize").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        return sc;
    }
    public static void main(String[] args) {
        List list = Arrays.asList(1,2,3,4);

        JavaSparkContext sc = getsc();

        final Broadcast broadcast =  sc.broadcast(10);

        JavaRDD rdd = sc.parallelize(list);

        JavaRDD values = rdd.map(new Function<Integer,Integer>() {
            @Override
            public Integer call(Integer value) throws Exception {
                return value * (int)broadcast.getValue();
            }
        });
        values.foreach(new VoidFunction() {
            @Override
            public void call(Object o) throws Exception {
                System.out.println(o);
            }
        });


        final  Accumulator accumulator = sc.accumulator(5);

        rdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer o) throws Exception {
                accumulator.add(o);
            }
        });

        System.out.println(accumulator.value());


    }
}
