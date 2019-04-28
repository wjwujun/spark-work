package sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
* 自定义排序
* */
object CustomSort1 {
  def main(args: Array[String]): Unit = {

      val conf: SparkConf = new SparkConf().setAppName("CustomSort1").setMaster("local")
      val sc = new SparkContext(conf)

      //排序规则：首先按照颜值的降序，如果颜值相等，再按照年龄的升序
      val users= Array("aa 30 99", "bb 29 9999", "cc 28 98", "dd 28 99")

      //将Driver端的数据并行化变成RDD
      val lines: RDD[String] = sc.parallelize(users)


    //切分整理数据
    val userRDD: RDD[User] = lines.map(line => {
      val fields = line.split(" ")
      val name = fields(0)
      val age = fields(1).toInt
      val fv = fields(2).toInt
      new User(name, age, fv)
    })

    //将RDD里面装的User类型的数据进行排序
    val sorted: RDD[User] = userRDD.sortBy(u => u)

    val r = sorted.collect()

    println(r.toBuffer)

    sc.stop()

  }


}

class User(val name: String, val age: Int, val fv: Int) extends Ordered[User] with Serializable {

  override def compare(that: User): Int = {
    if(this.fv == that.fv) {   //颜值相等 按年龄
      this.age - that.age
    } else {
      -(this.fv - that.fv)
    }
  }

  override def toString: String = s"name: $name, age: $age, fv: $fv"
}