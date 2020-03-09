package com.bw.ln.Search

import java.sql.Connection

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkMysqlInput {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)
    //创建rdd
    val data: RDD[String] = sc.parallelize(List("1","liu"))

    data.foreachPartition(insertData)


  }

  def  insertData(interator:Iterator[String])={
    Class.forName("com.mysql.jdbc.Driver").newInstance()

    val conn: Connection = java.sql.DriverManager.getConnection("jdbc:mysql://hadoop1:3306/stu","root","123456")


    interator.foreach(data=>{
      val ps=conn.prepareStatement("insert into t_student(id,name) values (?,?)")
      ps.setInt(1,data.toInt)

      ps.executeUpdate()
    })
  }

}

