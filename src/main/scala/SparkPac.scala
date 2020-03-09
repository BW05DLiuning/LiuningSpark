import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkPac {
//  def main(args: Array[String]): Unit = {

//    val sparkconf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
//
//
//    val sc = new SparkContext(sparkconf)
//
//
//    //读取文件
//    val line: RDD[String] = sc.textFile("infor/work.txt")
//    line.collect()
//
//    //压平
//    val word: RDD[String] = line.flatMap(_.split(" "))
//
//
//    val wordadnlone: RDD[(String, Int)] = word.map(x=>(x,1))
//
//
//    val wordandcount: RDD[(String, Int)] = wordadnlone.reduceByKey(_+_)
//
//
//    wordandcount.collect().foreach(println)
//    //保存
////    wordandcount.saveAsTextFile("C:\\Users\\liuni\\IdeaProjects\\LiuningSpark\\output")
//
//    sc.stop()
//
//
//  }

}
