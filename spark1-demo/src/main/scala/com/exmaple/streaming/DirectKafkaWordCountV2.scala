package com.exmaple.streaming

import com.exmaple.redis.JedisConnectionPool
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}

object DirectKafkaWordCountV2 {

  def main(args: Array[String]) {

    // 创建SparkConf
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    // 创建SparkStreaming，并设置间隔时间
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 指定组名
    val group = "g001"
    // 指定消费的 topic 名字
    val topic = "test"
    //创建 stream 时使用的 topic 名字集合，SparkStreaming可同时消费多个topic
    val topicsSet: Set[String] = Set(topic)
    // 指定kafka的broker地址
    val brokers = "hadoop2:9092,hadoop4:9092,hadoop6:9092"

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null

    // offsets:g001-test <=====> 0-offset;1-offset;2-offset
    val redisKey = "offsets:" + group + "-" + topic
    // 过去redis偏移数据
    val exist: Boolean = JedisConnectionPool.exists(redisKey)
    if (exist) { //
      var topicPartition: Map[TopicPartition, Long] = Map()

      val offsetStr: String = JedisConnectionPool.get(redisKey)
      if (offsetStr != null && offsetStr.trim.length > 0) {
        offsetStr.split(";").map(str => {
          val fileds = str.split("-")
          val parition: Int = fileds.head.toInt
          val offset: Long = fileds.last.toLong
          val topicPartition = new TopicPartition(topic, parition)
          (topicPartition -> offset)
        }).foreach(x => {
          if (x._2 > 0) {topicPartition += (x._1 -> x._2)}
        })
      }

      kafkaDStream = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams, topicPartition))
    } else {
      kafkaDStream = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
    }

    kafkaDStream.foreachRDD(kafkaRDD => {
      // ConsumerRecord => line
      val linesRDD : RDD[String] = kafkaRDD.map(x => x.value())

      linesRDD.foreachPartition(partitionLines => {
        partitionLines.foreach(partitionLine => {
          val date : String = partitionLine.substring(0, 13)
          JedisConnectionPool.incrBy(date,1)
        })
      })

      //只有KafkaRDD可以强转成HasOffsetRanges，并获取到偏移量
      val offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
      val offsetValue : StringBuffer = new StringBuffer()
      for (o <- offsetRanges) {
        // 将该 partition
        val partition = s"${o.partition}"
        // 将该 partition 的 offset ${o.untilOffset.toString}
        val untilOffset = o.untilOffset

        if (untilOffset > 0) {
          offsetValue.append(s"${partition}-${untilOffset};")
        }
      }

      if (offsetValue.length() > 0) {
        println(s"保存的路径为 -> ${redisKey}, 保存的偏移量为 -> ${offsetValue}")
        /** 偏移量 写入redis */
        JedisConnectionPool.set(redisKey, offsetValue.toString)
      }
    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}

// scalastyle:on println
