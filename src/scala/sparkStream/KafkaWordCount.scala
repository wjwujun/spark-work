package sparkStream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/*
* 消费kafka中的数据
* */
object KafkaWordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val zkQuorum = "host-01:2181"   //zookeeper地址,多个集群用‘,’隔开
    val groupId = "g1"                     //消费者名称
    val topic = Map[String, Int]("kkafka" -> 1)   //topic名字和对应的线程数量
    //创建DStream，需要KafkaDStream
    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topic)
    /*
    * 对数据进行处理
    * Kafak的ReceiverInputDStream[(String, String)]里面装的是一个元组（key是写入的key，value是实际写入的内容）
    * */
    val lines: DStream[String] = data.map(_._2)

    /*
    *
    * 对DSteam进行操作，你操作这个抽象（代理，描述），就像操作一个本地的集合一样
    * 切分压平
    * */
    val words: DStream[String] = lines.flatMap(_.split(" "))
    //单词和一组合在一起
    val wordAndOne: DStream[(String, Int)] = words.map((_, 1))

    //聚合
    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)
    //打印结果(Action)
    reduced.print()
    //启动sparksteaming程序
    ssc.start()
    //等待优雅的退出
    ssc.awaitTermination()


  }

}
