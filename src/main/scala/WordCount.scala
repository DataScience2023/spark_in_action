
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WordCount {

  def main(args: Array[String]): Unit = {

    // spark开发入口 SparkSession 实例
    val spark = SparkSession.builder()
      .appName("WordCount")
      .master("local[1]")
      .getOrCreate()

    // 文件路径
    val file: String = s"src/main/resources/people.txt"

    // 读取文件内容， RDD 弹性分布式数据集
    val lineRDD: RDD[String] = spark.sparkContext.textFile(file)

    // 以行为单位做分词
    val wordRDD: RDD[String] = lineRDD.flatMap(line => line.split(","))

    // 过滤掉空字符串
    val cleanWordRDD: RDD[String] = wordRDD.filter(word => !word.equals(""))

    // 把 RDD 元素 转换为 (key, value) 形式
    val kvRDD: RDD[(String, Int)] = cleanWordRDD.map(word => (word, 1))

    // 按照单词分组计数
    val wordCounts: RDD[(String, Int)] = kvRDD.reduceByKey((x, y) => x + y)

    // 打印词频最高的5个词汇
    wordCounts.map{case (k, v) => (v, k)}.sortByKey(ascending = false).take(5)
      .foreach(println)

    spark.stop()

  }

}
