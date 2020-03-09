package com.com.ln.kafka

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaHigehStreamingUpdate {

  def createSSc(): StreamingContext ={
    //创建配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("kafkaStreaming")

    val sc: StreamingContext = new StreamingContext(conf,Seconds(3))

    sc.checkpoint("./ck1")
    //kafka参数
    val brokers:String ="hadoop1:9092,hadoop2:9092,hadoop3:9092"
    //消费主题
    val topie:String="first"
    //组名
    val group:String="liuning"

    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"

    //封装kafka参数
    val kafkaPara: Map[String, String] = Map[String, String](
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )


    //读取kakfa数据  创建Dsream

    //指定kv类型 和解码器
    val kafkaStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](sc,
      kafkaPara,
      Set(topie)
    )
    //打印数据
    kafkaStream.print

    sc
  }

  def main(args: Array[String]): Unit = {

    //获取SSC
    val ssc = StreamingContext.getActiveOrCreate("./ck1",()=>createSSc())

    ssc.start()
    ssc.awaitTermination()
  }
}
