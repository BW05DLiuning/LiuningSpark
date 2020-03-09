package com.accu.TestAccu

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object TestAccu {
  def main(args: Array[String]): Unit = {


  //创建spark配置信息
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")

  //创建SparkContext
  val sc = new SparkContext(sparkConf)
  //定义累加器
  val sum: LongAccumulator = sc.longAccumulator("sum")


  val number: RDD[Int]= sc.parallelize(Array(1,2,3,4),2)

  val numbertwo: RDD[(Int, Int)] = number.map(x => {
    sum.add(1)

    (x, 1)
  })
  numbertwo.foreach(println)

    println("************************")
    println(sum.value)

    sc.stop()
  }
}
