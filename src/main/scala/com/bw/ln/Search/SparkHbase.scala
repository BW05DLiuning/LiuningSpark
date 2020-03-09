package com.bw.ln.Search

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
object SparkHbase {
  def main(args: Array[String]): Unit = {

    //创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")

    //创建SparkContext
    val sc = new SparkContext(sparkConf)

    //增加广播变量
    val ruleRDD: RDD[String] = sc.textFile("/var/data/tobroadcastInput")
    val rule: Broadcast[RDD[String]] = sc.broadcast(ruleRDD)

    //构建HBase配置信息
    val conf: Configuration = HBaseConfiguration.create()

    conf.set("hbase.zookeeper.quorum", "hadoop1,hadoop2,hadoop3")
    conf.set(TableInputFormat.INPUT_TABLE, "liuning")

    val hbaserdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    //打印数据
    hbaserdd.foreach {
      case (_, result) =>
        //小弟增加广播变量
        val rv = rule.value
        val key: String = Bytes.toString(result.getRow)
        val name: String = Bytes.toString(result.getValue(Bytes.toBytes("infor"), Bytes.toBytes("name")))
        val color: String = Bytes.toString(result.getValue(Bytes.toBytes("infor"), Bytes.toBytes("age")))
        println("RowKey:" + key + ",Name:" + name + ",Age:" + color)
    }
    sc.stop()
  }
}
