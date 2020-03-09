package com.com.ln.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamOne {
  def main(args: Array[String]): Unit = {
    //初始化Spark信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWord")

    val sc: StreamingContext = new StreamingContext(conf,Seconds(3))

    val lineStream: ReceiverInputDStream[String] = sc.socketTextStream("hadoop1",5566)

    val wordStream: DStream[String] = lineStream.flatMap(_.split(" "))

    val wordOneStream: DStream[(String, Int)] = wordStream.map(x=>(x,1))

    val WordCount: DStream[(String, Int)] = wordOneStream.reduceByKey(_+_)

    WordCount.print()


    sc.start()
    sc.awaitTermination()
  }
}
