package com.ln.pac

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SggSpark {
  def main(args: Array[String]): Unit = {
    //创建配置
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")

    //创建上下文环境
    val sc: SparkContext = new SparkContext(conf)

    //读取文件数据
    val lineList: RDD[String] = sc.textFile("infor/work.txt")

    //扁平化
    //    val wordRDD: RDD[String] = lineList.flatMap(_.split(" "))
    val wordRDD: RDD[String] = lineList.flatMap(line=>line.split(" "))

    //转换结构
    //    val wordMapRDD: RDD[(String, Int)] = wordRDD.map((_,1))
    val wordMapRDD: RDD[(String, Int)] = wordRDD.map(word=>(word,1))

    //reduceByKey做聚合
    //    val result: RDD[(String, Int)] = wordMapRDD.reduceByKey(_ + _)
    val result: RDD[(String, Int)] = wordMapRDD.reduceByKey((x, y)=>(x + y))

    //数据收集collect
    //控制台打印输出
    result.collect().foreach(println)

    //关闭资源
    sc.stop()
  }

}
