package com.bw.ln.Search

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}


object SparkHbaseInputTwo {
  def main(args: Array[String]): Unit = {
    //获取Spark配置信息并创建与spark的连接
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HBaseApp")
    val sc = new SparkContext(sparkConf)

    //创建HBaseConf
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum","hadoop1,hadoop2,hadoop3")
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "user")

    //定义往Hbase插入数据的方法
    def convert(triple: (Int, String, Int)) = {
      val put = new Put(Bytes.toBytes(triple._1))

      put.addImmutable(Bytes.toBytes("infor"), Bytes.toBytes("name"), Bytes.toBytes(triple._2))

      put.addImmutable(Bytes.toBytes("infor"), Bytes.toBytes("age"), Bytes.toBytes(triple._3))
      (new ImmutableBytesWritable, put)
    }

    //创建一个RDD
    val initialRDD = sc.parallelize(List((1,"apple",11), (2,"banana",12), (3,"pear",13)))

    //将RDD内容写到HBase
    val localData = initialRDD.map(convert)

    localData.saveAsHadoopDataset(jobConf)
  }
}
