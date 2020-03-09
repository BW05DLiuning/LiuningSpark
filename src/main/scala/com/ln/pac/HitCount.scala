package com.ln.pac

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HitCount {
  //自定义累加器
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HitCount")

    //创建上下文环境
    val sc: SparkContext = new SparkContext(conf)

    val information: RDD[String] = sc.textFile("infor/agent.log")

    val infor = information.map(x => {
      val strings: Array[String] = x.split(" ")
      ((strings(1).toInt, strings(4).toInt), 1)
    })

    val prpvinceGroup: RDD[((Int, Int), Int)] = infor.reduceByKey(_+_)

    val provinceAdtop: RDD[(Int, Iterable[Int])] = prpvinceGroup.map(x=>(x._1._1,(x._2))).groupByKey()



  }
}
