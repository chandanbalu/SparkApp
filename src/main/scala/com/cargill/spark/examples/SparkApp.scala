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

    val logFile = "C:\\Users\\c795701\\Desktop\\README.md" // Should be some file on your system

    val spark = SparkSession
      .builder
      .appName("Simple Application")
      .master("local")
      .getOrCreate()

    val logData = spark.read.textFile(logFile).cache()

    val numAs = logData.filter(line => line.contains("a")).count()

    val numBs = logData.filter(line => line.contains("b")).count()

    println(s"Lines with a: $numAs, Lines with b: $numBs")

    spark.stop()
  }
}