package com.ln.pac

import org.apache.spark.{SparkConf, SparkContext}

object SparkOperator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")

    //创建上下文环境
    val sc: SparkContext = new SparkContext(conf)


    //创建一个rdd 然后mapValue添加字符串
//    val rdd1=sc.parallelize(Array((1,"a"),(2,"b"),(1,"d"),(3,"c")))
//    rdd1.mapValues(_+"**").collect().foreach(println)

//  println("*******************************************************")

    //join  将两个rdd根据key如果相同  就将value放在一起
//    val rdd1=sc.parallelize(Array((1,"a"),(2,"b"),(1,"d"),(3,"c")))
//    val rdd2=sc.parallelize(Array((1,"enen"),(2,5),(3,0)))
//    rdd1.join(rdd2).collect().foreach(println)
    //  println("*******************************************************")


//    val rdd = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))
//
//    val rdd1 = sc.parallelize(Array((1,4),(2,5),(3,6)))
//    rdd.cogroup(rdd1).collect().foreach(println)

    //  reduce的用法
//    val rdd1 = sc.makeRDD(1 to 10,2)
//
//    println(rdd1.reduce(_ + _))
//
//    val rdd2 = sc.makeRDD(Array(("a",1),("a",3),("c",3),("d",5)))
//
//    val tuple: (String, Int) = rdd2.reduce((x, y)=>(x._1 + y._1,x._2 + y._2))
//
//    println(tuple)
  }
}
