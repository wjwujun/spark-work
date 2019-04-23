package top

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FavTeacher {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FavTeacher").setMaster("local")

    val context = new SparkContext(conf)

    //指定从哪里读取数据
    val lines: RDD[String] = context.textFile("hdfs://host-01:9000/teacher/")
    //整理数据
    val teacherAndOne = lines.map(line => {
          val index: Int = line.lastIndexOf("/")
          val teacher: String = line.substring(index+1)
          val host: String = line.substring(0,index)
          //返回
          (teacher,1)
    })

    //聚合
    val reduced: RDD[(String, Int)] = teacherAndOne.reduceByKey(_+_)
    //排序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    sorted.saveAsTextFile("hdfs://host-01:9000/teacher/clean")

    //触发Action执行计算
    //val reslut: Array[(String, Int)] = sorted.collect()
    //打印
    //println(reslut.toBuffer)

    context.stop()
  }
}
