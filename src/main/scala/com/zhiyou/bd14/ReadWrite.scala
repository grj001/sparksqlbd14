package com.zhiyou100.bd14

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

object ReadWrite {
  val spark = SparkSession.builder().master("local[*]").appName("sparksql存取文件").enableHiveSupport().getOrCreate()

  import spark.implicits._
  import spark.sql

  def saveCsv() = {
    val ds = spark.read.text("/user/orderdata/orders")
    ds.printSchema()
    val orderSplit = ds.map(x => {
      val info = x.getString(0).split("\\|")
      (info(0), info(1), info(2), info(3), 1)
    })
    orderSplit.printSchema()
    orderSplit.createOrReplaceTempView("orders")
    //获取订单状态为COMPLETE的订单信息
    val result = sql("select * from orders where _4='COMPLETE'")
    //    result.show()
    //Returns a new Dataset that has exactly `numPartitions` partitions
    result.coalesce(1)
    result.write.csv("/user/from-spark-order/ordercsv")

  }

  def readCsv() = {
    val ds = spark.read.csv("/user/from-spark-order/ordercsv")
    ds.printSchema()
    //    ds.show()
    ds.createOrReplaceTempView("orders")
    sql(
      """
        |select _c0
        |         ,_c3
        |from orders
      """.stripMargin).show()
  }


  //parquet格式, 列的形式
  def writeParquet() = {
    val ds = spark.read.text("/user/orderdata/orders")
    ds.printSchema()
    val orderSplit = ds.map(x => {
      val info = x.getString(0).split("\\|")
      (info(0), info(1), info(2), info(3), 1)
    })
    orderSplit.printSchema()
    orderSplit.write.parquet("/user/from-spark-order/parquet")
  }

  def readParquet() = {
    val ds = spark.read.parquet("/user/from-spark-order/parquet")
    ds.printSchema()
  }


  //orc
  def writeOrc() = {
    val ds = spark.read.text("/user/orderdata/orders")
    val orderSplit = ds.map(x => {
      val info = x.getString(0).split("\\|")
      ((info(0), info(1), info(2), info(3)), 1)
    })
    orderSplit.write.orc("/user/from-spark-order/orc")
  }

  def readOrc() = {
    val ds = spark.read.orc("/user/from-spark-order/orc")
    ds.printSchema()
  }


  //json
  def writeJson() = {
    val ds = spark.read.text("/user/orderdata/orders")
    val orderSplit = ds.map(x => {
      val info = x.getString(0).split("\\|")
      ((info(0), info(1), info(2), info(3)), 1)
    })
    orderSplit.write.json("/user/from-spark-order/json")
  }
  def readJson() = {
    val ds = spark.read.json("/user/from-spark-order/json")
    ds.printSchema()
  }

















  //jdbc
  def readJdbc() = {
    val url = "jdbc:mysql://localhost:3306/orderdata"
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "root")
    val ds = spark.read.jdbc(url, "departments", properties)
    ds.printSchema()
    ds.show()
  }


  def writeJdbc() = {
    val ds = spark.read.text("/user/orderdata/orders")
    val orderSplit = ds.map(x => {
      val info = x.getString(0).split("\\|")
      (info(0), info(1), info(2), info(3),1)
    })
    orderSplit.createOrReplaceTempView("orders1")

    val complete = sql("select * from orders1 where _4 = 'COMPLETE'")
    val url = "jdbc:mysql://localhost:3306/bigdata14"
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "root")

    complete.write.jdbc(url, "orders1", properties)
  }





  def saveMode() = {
    val ds = spark.read.text("/user/orderdata/orders")
    val orderSplit = ds.map(x => {
      val info = x.getString(0).split("\\|")
      (info(0), info(1), info(2), info(3))
    })
    orderSplit.createOrReplaceTempView("orders")
    val complete = sql(
      "select * from order where _4 = 'COMPLETE'"
    )
    //重写
    complete.write.mode(SaveMode.Overwrite).parquet("/user/from-spark-order/orderdata/orders")
    //追加
    //    complete.write.mode(SaveMode.Append).parquet("/user/from-spark-order")
  }


  def main(args: Array[String]): Unit = {
    //    saveCsv()
    //        readCsv()
    //    writeParquet()
    //    writeOrc()
//    readOrc()
//    writeJson()
//    readJson()

        readJdbc()
//        writeJdbc()

    //    saveMode()


  }
}

