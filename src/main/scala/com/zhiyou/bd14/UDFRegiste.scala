package com.zhiyou.bd14

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

object UDFRegiste {

  val spark = SparkSession.builder().master("local[*]")
    .appName("udf").getOrCreate()

  import spark.implicits._
  import spark.sql

  def udfTest() = {
    val rdd = spark.sparkContext.textFile("/user/orderdata/orders")
    val ds = rdd.map(x => {
      val info = x.split("\\|")
      Order(info(0),info(1),info(2),info(3))
    }).toDS()

    //自定义一个函数把2014-02-22 00:00:00 日期转换成2014-20-22格式
    spark.udf.register("my_dt_to_date"
    , (dateTime:String) => {
        val sformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val tformat = new SimpleDateFormat("yyyy-MM-dd")
        val date = sformat.parse(dateTime)
        tformat.format(date)
      })

    ds.createOrReplaceTempView("orders")
    val useUdf = sql(
      """
        |select orderId
        |       , my_dt_to_date(orderDate)
        |       , customerId
        |       , status
        |from orders
      """.stripMargin
    )
    useUdf.printSchema()
    useUdf.show()
  }


  def main(args: Array[String]): Unit = {
    udfTest()
  }
}
