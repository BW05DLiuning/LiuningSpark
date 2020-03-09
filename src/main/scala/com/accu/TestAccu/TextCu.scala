package com.accu.TestAccu

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TextCu {
  def main(args: Array[String]): Unit = {
    //创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")

    //创建SparkContext
    val sc = new SparkContext(sparkConf)
    //定义累加器

    var customer = new Customer

    sc.register(customer,"sum")

    val number: RDD[Int]= sc.parallelize(Array(1,2,3,4),2)

    number.map(x=>{
      customer.add(x)
    x
    }).collect()

    //打印累加器值
    println(customer.value)

    sc.stop()

  }
}
