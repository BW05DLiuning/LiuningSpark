package com.bw.ln.Search

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkMysql {
  def main(args: Array[String]): Unit = {

    //1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop1:3306/stu"
    val userName = "root"
    val passWd = "123456"

    val jdbcrdd: JdbcRDD[Int] = new JdbcRDD[Int](
      sc,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url, userName, passWd)
      },
      "select * from t_student where ? <= id and id <= ?;",
      1,
      3,
      1,
      (x: ResultSet) => x.getInt(1))
    jdbcrdd.foreach(println)


      sc.stop()

  }
}
