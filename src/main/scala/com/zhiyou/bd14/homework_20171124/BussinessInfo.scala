package com.zhiyou.bd14.homework_20171124

import java.sql.{DriverManager, ResultSet}

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.spark.rdd.{JdbcRDD, PairRDDFunctions, RDD}
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

object BussinessInfo {

  //统计出每个用户的订单数, 消费总金额, 购买过的产品类型的数量
  //                   如果一个用户, 总共下过2个订单, 第一个订单里面买了手机 男装
  //                                                第二个订单里面买了手机 女装
  //                                                购买的产品的类型数量是 3
  //                                                2. 统计每个店铺销售的商品品类的数
  //                                                量, 总销售额

  /*
  每个用户的订单数, customers, products
   */

  val conf = new SparkConf().setMaster("local[*]")
    .setAppName("Bussine Infomations")
  val sc = SparkContext.getOrCreate(conf)

  val url = "jdbc:mysql://localhost:3306/orderdata"
  val username = "root"
  val password = "root"

  val getMysqlConnection = () => {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection(url, username, password)
  }

  //////////////////////////////////////////////////////////////////////////////

  def getCustomersInfoFromMysql() = {
    val sql = "select * from customers where customer_id >= ? and customer_id <=?"
    val mysqlRDD = new JdbcRDD(sc, getMysqlConnection, sql, 1, 68883, 2, x => x)

    mysqlRDD.foreach(x => {
      println(s"${x.getInt("customer_id")}")
    })
  }

  def getCustomersRDD(): RDD[(String, String)] = {
    val sql = "select * from customers where customer_id >= ? and customer_id <=?"
    val mysqlRDD = new JdbcRDD(sc, getMysqlConnection, sql, 1, 68883, 2, x => {
      (x.getInt("customer_id").toString -> x.getString("customer_fname"))
    })
    mysqlRDD
  }

  ///////////////////////////////////////////////////////////////////////////////

  def getOrdersInfoFromMysql() = {
    val sql = "select * from orders where order_id>=? and order_id<=?"
    val mysqlRDD = new JdbcRDD(sc, getMysqlConnection, sql, 1, 12435, 2, x => x)


    mysqlRDD.foreach(x => {
      println(s"${x.getInt("order_id")}")
    })
  }

  def getOrdersRDD(): RDD[(String, String)] = {
    val sql = "select * from orders where order_id>=? and order_id<=?"
    val mysqlRDD = new JdbcRDD(sc, getMysqlConnection, sql, 1, 12435, 2, x => {
      (x.getInt("order_customer_id").toString -> x.getInt("order_id").toString)
    })
    mysqlRDD
  }


  //做外关联
  def getOrdersNumForOneCustomer() = {

    val customersRDD = getCustomersRDD()
    val ordersRDD = getOrdersRDD()


    //左外关联
    val ordersLeftJoinCustomerResult = ordersRDD.leftOuterJoin(customersRDD)
    //订单表左外关联用户表, 得到多有的关联信息
    //filter, 得到每个用户的信息
    //order_id order_customer_id, customer_id customer_id
    //RDD的左连接是将, 拥有相同Key的rdd的value连接在一起, 左边的在左边,右边的在右边
    // 左orders                        右customers
    // order_customer_id,order_id      customer_id,customer_id
    ordersLeftJoinCustomerResult.foreach(x => {
      val customerFname = x._2._2 match {
        case None => "没有数据"
        case Some(a) => a
      }
//      println(x)
//      println(s"用户id:${x._1}, 订单号:${x._2._1}, 用户名称:${customerFname}")
    })

    val ordersNumForOneCustomerResult = ordersLeftJoinCustomerResult.map(x => {
      //计算用户的订单数
      //key为用户id, value为订单id
      //(customer_id , (order_id, customer_fname))
      (x._2._1, x._1)
    }).reduceByKey(_+_).sortBy(x =>x._1.toInt,false,1)
    ordersNumForOneCustomerResult.foreach(x => {
      println(s"用户id:${x._1}, 订单数量:${x._2}")
    })

    ordersNumForOneCustomerResult.saveAsHadoopFile(
      "/user/spark-orderdata/ordersnum-for-onecustomer"
      , classOf[Text]
      , classOf[Text]
      , classOf[TextOutputFormat[Text,Text]])

  }


  def main(args: Array[String]): Unit = {
    //    getCustomersInfoFromMysql()
    getOrdersNumForOneCustomer()
  }


}
