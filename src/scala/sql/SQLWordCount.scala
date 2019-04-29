package sql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/*
* wordCount
* */
object SQLWordCount {
  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val spark = SparkSession.builder()
      .appName("SQLWordCount")
      .master("local[*]")
      .getOrCreate()


    /*
    *
    * (指定以后从哪里)读数据，是lazy
    *  Dataset分布式数据集，是对RDD的进一步封装，是更加智能的RDD
    *  dataset只有一列，默认这列叫value
    * */
    val lines: Dataset[String] = spark.read.textFile("E:/data/a.txt")
    //lines.show();

    /*
    * 整理数据(切分压平)
    * 导入隐式转换
    * flatMap是rdd中的方法，所以在Dataset中使用的时候，需要隐式转换一下
    * */
    import spark.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(" "))
    //words.show()

    //注册视图
    words.createTempView("v_wordCount")

    //执行SQL（Transformation，lazy）
    val result: DataFrame = spark.sql("SELECT value, COUNT(*) counts FROM v_wordCount GROUP BY value ORDER BY counts DESC")

    //执行Action
    result.show()
    spark.stop()

  }
}
