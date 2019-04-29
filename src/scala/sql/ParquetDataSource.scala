package sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/*
*
*读取Parquet数据
* */
object ParquetDataSource {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ParquetDataSource")
      .master("local[*]")
      .getOrCreate()


    //指定以后读取数据
    val parquetLine: DataFrame = spark.read.parquet("/Users/zx/Desktop/parquet")
    //val parquetLine: DataFrame = spark.read.format("parquet").load("/Users/zx/Desktop/pq")
    parquetLine.printSchema()

    //show是Action
    parquetLine.show()

    spark.stop()

  }
}
