import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingWordCount {
  def main(args: Array[String]) {

    println("start word count")

    //创建流执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //定义流数据来源
    val text = env.socketTextStream("singlenode", 9999)
    //val text = env.readTextFile("s3a://somebucket/prefix")


    //senv.addSource()

    //计算过程并将结果放入变量中
    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)

    //将数据写入到标准输出，cat flink-root-taskexecutor-0-singlenode.ou
    counts.print()
    println("end word count")

    //触发流执行
    env.execute("Window Stream WordCount")
    println("exit now!")
  }
}
