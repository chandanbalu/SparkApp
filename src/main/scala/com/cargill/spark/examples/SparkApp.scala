package com.cargill.spark.examples

/**
 * Created by IntelliJ IDEA.
 *
 * @author : Chandan Balu
 * @created_date : 6/9/2020, Tue
 **/
//https://github.com/spirom/LearningSpark

import org.apache.spark.sql.SparkSession

object SimpleApp {
  /* SimpleApp.scala */
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("Simple Application")
      .master("local")
      .getOrCreate()

    val resourcePath = "src/main/resources/titanic.csv"
    //val fileName = new FileSystem().getFilePath("README.md")

    val logData = spark.read.textFile(resourcePath).cache()

    val numAs = logData.filter(line => line.contains("a")).count()

    val numBs = logData.filter(line => line.contains("b")).count()

    println(s"Lines with a: $numAs, Lines with b: $numBs")

    spark.stop()

  }
}