package com.zhiyou.bd14

import java.util

import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


case class Student(name:String,gender:String,age:Int)
case class Order(orderId:String,OrderDate:String,customerId:String,status:String)

object SchemaAdd {

  //自动加载      1. 文件中自动加载模式
  //             2. 数据中自动加载模式
  val spark = SparkSession.builder().master("local[*]")
    .appName("add Schema").getOrCreate()

  import spark.implicits._
  import spark.sql

  def addSchemaFromData() = {
    val list = List(1, 2, 3, 4, 5)
    val rdd = spark.sparkContext.parallelize(list)
    val ds1 = rdd.toDS()

//    ds1.printSchema()

    val ds2 = spark.createDataset(list)
//    ds2.printSchema()

    val list2 = List((1, 2, 3), (4, 5, 6), (7, 8, 9))
    val ds3 = spark.createDataset(list2)
    ds3.printSchema()
  }

  //手动给元组添加模式
  def addSchemaByFrame() = {
    val list = List(1, 2, 3, 4, 5)
    import scala.collection.JavaConversions._
    val structType = StructType(StructField("column1", IntegerType, true) :: Nil)
    //createDataFrame再把list转换成ds是要求的list类型是java上的util.List类型
    val ds1 = spark.createDataFrame(list.map(x => Row(x)), structType)
    ds1.printSchema()
    ds1.createOrReplaceTempView("list01")
//    ds1.show()



    val students = List(Row("小张", "男", 18), Row("小李", "男", 23), Row("小网", "男", 12))


    val studentSche =
      StructType(
        StructField("name", StringType, true) ::
          StructField("gender", StringType, true) ::
          StructField("age", IntegerType, true) :: Nil
      )

//    val studentSchema1 = new StructType()
//    studentSchema1.add(StructField("name",StringType,true))
//    studentSchema1.add(StructField("gender",StringType,true))
//    studentSchema1.add(StructField("age",IntegerType,true))





    val ds2 = spark.createDataFrame(students, studentSche)
    ds2.printSchema()
    ds2.createOrReplaceTempView("student")
//    sql("select * from student where age >= 18").show()
  }


  //case class 的方式给数据添加模式
  def caseClassSchema() = {
    val list = List(Student("小张","男",11),Student("小2","男",31),Student("小3","男",12))
    val ds1 = spark.createDataset(list)
    ds1.printSchema()
    ds1.createOrReplaceTempView("student")
    sql("select * from student").show()
  }


  def caseClassSchemaRDD() = {
    val rdd = spark.sparkContext.textFile("/user/orderdata/orders")
    val ccRDD = rdd.map(x => {
      val info = x.split("\\|")
      Order(info(0),info(1),info(2),info(3))
    })
    val ds = ccRDD.toDS()
    ds.printSchema()
    ds.createOrReplaceTempView("orders")
    //统计每个客户的订单数
    sql(
      """
        |select customerId
        |        , count(1)
        |from orders
        |group by customerId
      """.stripMargin
    ).show()
    val df = ds.toDF()
    df.printSchema()
    df.take(10).foreach(println)
  }



  def main(args: Array[String]): Unit = {
//        addSchemaFromData()
//    addSchemaByFrame()
//    caseClassSchema()
    caseClassSchemaRDD()
  }


}
