package com.ln.pac

import com.bw.ln.Search.Search
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SeriTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    //创建一个rdd
    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "liuning"))

    val search = new Search("hadoop")

    val fileted:  RDD[String] = search.getMatch1(rdd)

    fileted.foreach(println)

    sc.stop()
  }


}
