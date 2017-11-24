package com.zhiyou.bd14.homework_20171124

import java.io.{File, FileFilter}

object HomeWork_20171124 {

  //每个表的sql
  def oneTableSql(tableName: String) = {

    //sql语句生成
    val parent =
      new File("C:\\Users\\Administrator\\Desktop\\SVN\\orderdata\\orderdata\\" + tableName)

    val fileArray = parent.listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = {
        if (pathname.getName == "_SUCCESS") {
          false
        } else {
          true
        }
      }
    })
    fileArray.foreach(x => {
      val path = x.getAbsolutePath.replace("\\","\\\\")
      val sqlStr = StringBuilder.newBuilder
      sqlStr.append(" load data INFILE ")
      sqlStr.append(" '" + path + "' ")
      sqlStr.append(" into table ")
      sqlStr.append(" " + tableName + " ")
      sqlStr.append(" fields terminated by '|'; ")
      println(sqlStr)
    })
  }

  //遍历表名
  def getTableNames() = {
    val file = new File("C:\\Users\\Administrator\\Desktop\\SVN\\orderdata\\orderdata")
    val fileArray = file.listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = true
    })
    fileArray
  }

  def main(args: Array[String]): Unit = {
    getTableNames().foreach(x => {
      println(x.getName)
      oneTableSql(x.getName)
    })

  }
}
