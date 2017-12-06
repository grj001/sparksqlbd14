package com.zhiyou.bd14

import org.apache.spark.sql.SparkSession

object DateSetApi {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("ds api test")
    .getOrCreate()

  import spark.implicits._
  import spark.sql

  def apiExper() = {
    val list = List(Student("张三","男",32),
      Student("李四","男",42)
    , Student("王五","女",34))

    val ds = spark.createDataset(list)
    ds.printSchema()

    //年龄大于19的
    val r1 = ds.filter("age>19").filter("gender=='男'")
//    r1.show()

    val r2 = ds.filter($"age">19).filter($"gender"==="男")
//    r2.show()

    val r3 = ds.select($"age",$"name").where($"gender"==="男")
//    r3.show()

    val r4 = ds.groupBy().max("age")
//    r4.show()

    val r5 = ds.agg(Map("age"->"max","age"->"min","age"->"avg"))
    r5.show()
  }



  def main(args: Array[String]): Unit = {
    apiExper()
  }

































}
