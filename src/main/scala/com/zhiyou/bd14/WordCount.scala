package com.zhiyou.bd14

import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {
    //构建sparkSession对象
    val spark =
      SparkSession.builder()
        .master("local[*]")
        .appName("spark sql word count")
        .getOrCreate()

    //引入隐式转换
    import spark.implicits._
    import spark.sql

    //构建rdd
    val rdd = spark.sparkContext.textFile("/user/user-logs-large.txt")
    val ds = rdd.toDS()

    //打印挡墙ds的模式
    ds.printSchema()
    //对ds中的数据进行sql的形式来处理
    //把ds创建一个视图名, 这个视图名就可以单座成表明来使用
    ds.createOrReplaceTempView("line_str")
    val wcResult = sql(
      """
        |select word
        |       , count(1) as count
        |from (
        |         select explode(split(value, ' ')) as word
        |         from line_str
        |)
        |group by word
      """.stripMargin)
    wcResult.show()
  }
}
