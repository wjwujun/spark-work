package top

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/*
*过滤方法对结果进行古过滤
*
* */
object GroupFavTeacher4{
  def main(args: Array[String]): Unit = {

    val topN = args(1).toInt
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


    //计算有多少学科
    val subjects: Array[String] = sbjectTeacherAndOne.map(_._1._1).distinct().collect()

    //自定义一个分区器，并且按照指定的分区器进行分区
    val sbPatitioner = new SubjectParitioner2(subjects)

    //聚合，聚合是就按照指定的分区器进行分区
    //该RDD一个分区内仅有一个学科的数据
    val reduced: RDD[((String, String), Int)] = sbjectTeacherAndOne.reduceByKey(sbPatitioner, _+_)

    //如果一次拿出一个分区(可以操作一个分区中的数据了)
    val sorted: RDD[((String, String), Int)] = reduced.mapPartitions(it => {
      //将迭代器转换成list，然后排序，在转换成迭代器返回
      it.toList.sortBy(_._2).reverse.take(topN).iterator

    })

    //收集结果
    //val r: Array[((String, String), Int)] = sorted.collect()
    //println(r.toBuffer)

    sorted.saveAsTextFile("hdfs://host-01:9000/teacher/clean")
    context.stop()
  }
}

//自定义分区器
class SubjectParitioner2(sbs: Array[String]) extends Partitioner {

  //相当于主构造器（new的时候回执行一次）
  //用于存放规则的一个map
  val rules = new mutable.HashMap[String, Int]()
  var i = 0
  for(sb <- sbs) {
    //rules(sb) = i
    rules.put(sb, i)
    i += 1
  }

  //返回分区的数量（下一个RDD有多少分区）
  override def numPartitions: Int = sbs.length

  //根据传入的key计算分区标号
  //key是一个元组（String， String）
  override def getPartition(key: Any): Int = {
    //获取学科名称
    val subject = key.asInstanceOf[(String, String)]._1
    //根据规则计算分区编号
    rules(subject)
  }
}
