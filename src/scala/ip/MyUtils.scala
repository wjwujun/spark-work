package ip


import java.sql.{Connection, DriverManager, PreparedStatement}

import scala.io.{BufferedSource, Source}


object MyUtils {

  def main(args: Array[String]): Unit = {
    //数据是在内存中
    val rules: Array[(Long, Long, String)] = readRules("E:/ip.txt")

    //将ip地址转换成十进制
    val ipNum = ip2Long("114.215.43.42")
    //2分查找出ip的位置
    val index = binarySearch(rules, ipNum)
    //根据脚本到rules中查找对应的数据
    val tp = rules(index)
    val no1 = tp._1
    val no2 = tp._2
    val province = tp._3
    println(no1)
    println(no2)
    println(province)

  }

  /*
  * 读取ip规则将规则放入内存中备用
  * */
 def readRules(path: String): Array[(Long, Long, String)] = {
    //读取ip规则
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()   // 读取文件所有行,获得一个迭代器
    //对ip规则进行整理，并放入到内存
    val rules: Array[(Long, Long, String)] = lines.map(line => {
      val fileds = line.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      (startNum, endNum, province)
    }).toArray
    rules
  }

  /*
 * 将IP转化成10进制数据，便于比较
 * */
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length){
      ipNum =  fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  /*
  * 2分查找,查找出要求的ip的数据
  * */
  def binarySearch(lines: Array[(Long, Long, String)], ip: Long) : Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1) && (ip <= lines(middle)._2))
        return middle
      if (ip < lines(middle)._1)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }




}
