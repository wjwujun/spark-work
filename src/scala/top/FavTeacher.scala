package top

import org.apache.spark.SparkConf

object FavTeacher {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FavTeacher").setMaster("local[4]")

  }
}
