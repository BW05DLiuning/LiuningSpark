package com.com.ln.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.immutable.HashMap



object KafkLowStreaming {

//  //获取offset
  def getOffset(kafkaCluset: KafkaCluster, group: String, topie: String): Map[TopicAndPartition, Long] = {

    //定义最终返回值  主题分区
    var partitionToLong = new HashMap[TopicAndPartition, Long]()
    //根据指定的topic获取分区
    val topicAndEither: Either[Err, Set[TopicAndPartition]] = kafkaCluset.getPartitions(Set(topie))
    //判断分区是否存在
    if (topicAndEither.isRight) {
      //分区信息不为空 取出信息
      val topicAndParatitions: Set[TopicAndPartition] = topicAndEither.right.get

      //获取消费者消费的进度
      val topicAndParatitionstoLongEither: Either[Err, Map[TopicAndPartition, Long]] = kafkaCluset.getConsumerOffsets(group, topicAndParatitions)

      //判断offset 是否存在
      if (topicAndParatitionstoLongEither.isLeft) {
        //offset不存在 未消费  遍历分区
        for (topicAndParatition <- topicAndParatitions) {
          //置为0
          //最好获取该分区最小offset
          partitionToLong += (topicAndParatition -> 0L)
        }
      } else {
        //取出offset
        val value: Map[TopicAndPartition, Long] = topicAndParatitionstoLongEither.right.get
        //赋值
        partitionToLong ++= value
      }
    }
     partitionToLong
  }


  //保存offset
  def setOffset(kafkaCluset: KafkaCluster, group: String,kafkalow: InputDStream[String]) = {


    kafkalow.foreachRDD(rdd=>{
      var partitionToLong = new HashMap[TopicAndPartition,Long]()

      //取出rdd当中的offset
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges]

      //获取所有分区的offsetRange
      val ranges: Array[OffsetRange] = offsetRanges.offsetRanges

      //遍历数组
      for (range<-ranges){
        partitionToLong+=(range.topicAndPartition()->range.untilOffset)


      }
      kafkaCluset.setConsumerOffsets(group,partitionToLong)

    })



  }


  def main(args: Array[String]): Unit = {
    //创建配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("kafkaStreamingLow")

    val sc: StreamingContext = new StreamingContext(conf,Seconds(3))

    sc.checkpoint("./ck")

    //kafka参数
    val brokers:String ="hadoop1:9092,hadoop2:9092,hadoop3:9092"
    //消费主题
    val topie:String="zuqing"
    //组名
    val group:String="liuning"

    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"

    //封装kafka参数
    val kafkaPara: Map[String, String] = Map[String, String](
      "zookeeper.connect"->"hadoop1:2181,hadoop2:2181,hadoop3:2181",
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )
    val kafkaCluset: KafkaCluster = new KafkaCluster(kafkaPara)


    val fromOffset: Map[TopicAndPartition, Long] = getOffset(kafkaCluset,group,topie)

    //消费kafka数据 创建Dstream
    val kafkalow: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](sc,
      kafkaPara,
      //获取当前消费的信息
      fromOffset,
      (message:MessageAndMetadata[String,String])=> message.message()
    )
    kafkalow.print()
  
    //保存offset
    setOffset(kafkaCluset,group,kafkalow)

    sc.start()
    sc.awaitTermination()
  }
}
