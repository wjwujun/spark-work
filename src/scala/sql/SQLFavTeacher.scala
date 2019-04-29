package sql

import java.net.URL

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/*
*
* 寻找最受欢迎的老师
* */
object SQLFavTeacher {
  def main(args: Array[String]): Unit = {

    val topN = 10

    val spark = SparkSession.builder().appName("SQLFavTeacher")
      .master("local[*]")
      .getOrCreate()

    val lines: Dataset[String] = spark.read.textFile("E:/data/teacher.log")
    import spark.implicits._

    val df: DataFrame = lines.map(line => {
      val tIndex = line.lastIndexOf("/") + 1
      val teacher = line.substring(tIndex)
      val host = new URL(line).getHost
      //学科的index
      val sIndex = host.indexOf(".")
      val subject = host.substring(0, sIndex)
      (subject, teacher)
    }).toDF("subject", "teacher")

    df.createTempView("v_sub_teacher")


    //该学科下的老师的访问次数
    val temp1: DataFrame = spark.sql("SELECT subject, teacher, count(*) counts FROM v_sub_teacher GROUP BY subject, teacher")

    //求每个学科下最受欢迎的老师的topn
    temp1.createTempView("v_temp_sub_teacher_counts")

    //val temp2 = spark.sql(s"SELECT * FROM (SELECT subject, teacher, counts, row_number() over(partition by subject order by counts desc) sub_rk, rank() over(order by counts desc) g_rk FROM v_temp_sub_teacher_counts) temp2 WHERE sub_rk <= $topN")

    //val temp2 = spark.sql(s"SELECT *, row_number() over(order by counts desc) g_rk FROM (SELECT subject, teacher, counts, row_number() over(partition by subject order by counts desc) sub_rk FROM v_temp_sub_teacher_counts) temp2 WHERE sub_rk <= $topN")

    val temp2 = spark.sql(s"SELECT *, dense_rank() over(order by counts desc) g_rk FROM (SELECT subject, teacher, counts, row_number() over(partition by subject order by counts desc) sub_rk FROM v_temp_sub_teacher_counts) temp2 WHERE sub_rk <= $topN")


    temp2.show()



    spark.stop()
  }

}
