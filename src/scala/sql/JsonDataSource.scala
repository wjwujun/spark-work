package sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/*
* 读取json数据
* */
object JsonDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JsonDataSource")
      .master("local[*]")
      .getOrCreate()


    import spark.implicits._
    //指定以后读取json类型的数据(有表头)
    val jsons: DataFrame = spark.read.json("/Users/zx/Desktop/json")
    val filtered: DataFrame = jsons.where($"age" <=500)

    filtered.printSchema()
    filtered.show()

    spark.stop()

  }
}
