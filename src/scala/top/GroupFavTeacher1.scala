package top

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupFavTeacher1 {
  def main(args: Array[String]): Unit = {

    // val topN = args(1).toInt
    val conf = new SparkConf().setAppName("FavTeacher").setMaster("local")
    val context = new SparkContext(conf)


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

    lines.map(line => {
      val index = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)
      val httpHost = line.substring(0, index)
      ((httpHost, teacher), 1)
    })

    //和一组合在一起(不好，调用了两次map方法)
    //val map: RDD[((String, String), Int)] = sbjectAndteacher.map((_, 1))

    //聚合，将学科和老师联合当做key
    val reduced: RDD[((String, String), Int)] = sbjectTeacherAndOne.reduceByKey(_+_)

    /**
    * 分组排序（按学科进行分组）
    * [学科，该学科对应的老师的数据]
    * (_._1._1)
    *    第一个_   相当于 ((subject, teacher), 1)
    *    第一个._1 相当于 (subject, teacher)
    *    第二个._1 相当于 subject
    *
    * groupByKey(numPartitions):按Key进行分组，返回[K,Iterable[V]]
    *
    *
    * */
    //val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy((t: ((String, String), Int)) =>t._1._1, 4)
    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)



    /**
    *
    *  经过分组后，一个分区内可能有多个学科的数据，一个学科就是一个迭代器
    * 将每一个组拿出来进行操作
    * 为什么可以调用sacla的sortby方法，因为一个学科的数据已经在一台机器上的一个scala集合里面了
    *
    * */
    val sorted = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(10))

    sorted.saveAsTextFile("hdfs://host-01:9000/teacher/clean")

    //触发Action执行计算
    //val reslut: Array[(String, Int)] = sorted.collect()
    //打印
    //println(reslut.toBuffer)

    context.stop()
  }
}
