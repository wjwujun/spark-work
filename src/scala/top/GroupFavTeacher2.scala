package top

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
*过滤方法对结果进行古过滤
*
* */
object GroupFavTeacher2{
  def main(args: Array[String]): Unit = {

    val topN = args(1).toInt
    val conf = new SparkConf().setAppName("FavTeacher").setMaster("local")
    val context = new SparkContext(conf)
    val subjects = Array("bigdata", "javaee", "php")

    //指定从哪里读取数据
    val lines: RDD[String] = context.textFile("hdfs://host-01:9000/teacher/")

    //整理数据
    val sbjectTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val index = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)
      val httpHost = line.substring(0, index)
      val subject = new URL(httpHost).getHost.split("[.]")(0)
      ((subject, teacher), 1)
    })

    //聚合，将学科和老师联合当做key
    val reduced: RDD[((String, String), Int)] = sbjectTeacherAndOne.reduceByKey(_+_)

    //scala的集合排序是在内存中进行的，但是内存有可能不够用
    //可以调用RDD的sortby方法，内存+磁盘进行排序
    for (sub <- subjects) {
      //该RDD中对应的数据仅有一个学科的数据（因为过滤过了）
      val filtered: RDD[((String, String), Int)] = reduced.filter(_._1._1 == sub)

      //现在调用的是RDD的sortBy方法，(take是一个action，会触发任务提交)
      val favTeacher = filtered.sortBy(_._2, false).take(topN)

      //打印
      println(favTeacher.toBuffer)
    }


    //触发Action执行计算
    //val reslut: Array[(String, Int)] = sorted.collect()
    //打印
    //println(reslut.toBuffer)

    context.stop()
  }
}
