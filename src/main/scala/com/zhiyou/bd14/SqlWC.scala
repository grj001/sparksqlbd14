package com.zhiyou.bd14

import org.apache.spark.sql.SparkSession

object SqlWC {


  val spark = SparkSession.builder()
      .master("local[*]")
    .appName("spark ds wc")
    .getOrCreate()

  import spark.implicits._
  import spark.sql

  def main(args: Array[String]): Unit = {
    val ds = spark.read.text("/user/user-logs-large.txt")
    ds.printSchema()
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
    wcResult.printSchema()
    wcResult
        .map(x => {
          s"${x.getString(0)}, ${x.getLong(1)}"
        })
      .write.text("/user/from-spark")
  }
}
