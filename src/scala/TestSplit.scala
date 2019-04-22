import java.net.URL

object TestSplit {
  def main(args: Array[String]): Unit = {
    val line = "http://bigdata.edu360.cn/laozhao"
    val index = line.lastIndexOf("/")
    val teacher = line.substring(index + 1)
    val httpHost = line.substring(0, index)

    val subject = new URL(httpHost).getHost.split("[.]")(0)

    println(teacher + ", " + subject)


  }
}
