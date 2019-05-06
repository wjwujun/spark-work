package sparkStream

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}



/*
*
* stream只会拉取,服务端新产生的数据，以前产生的数据不会拉取
*  直接消费socket服务端中产生的数据
* */
object SteamingWordCount {

  def main(args: Array[String]): Unit = {
    /*
    * 离线任务是创建SparkContext，现在要实现实时计算，用StreamingContext
    * local[2] 线程必须多个，一个用来接收数据，其余的用来计算
    * */

    val conf: SparkConf = new SparkConf().setAppName("SteamingWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    /*
    *StreamingContext是对SparkContext的包装，包了一层就增加了实时的功能
    * 第二个参数是小批次产生的时间间隔,每隔5秒钟拉取一次数据
    */
    val ssc = new StreamingContext(sc,Milliseconds(5000))
    /*
    * 有了StreamingContext，就可以创建SparkStreaming的抽象了DSteam
    * 从一个socket端口中读取数据
    * */
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.255.129", 8888)
    /*
    *对DSteam进行操作，你操作这个抽象（代理，描述），就像操作一个本地的集合一样
    *切分压平
    * */
    val words: DStream[String] = lines.flatMap(_.split(" "))
    //单词和一组合在一起
    val wordAndOne: DStream[(String, Int)] = words.map((_,1))
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
























