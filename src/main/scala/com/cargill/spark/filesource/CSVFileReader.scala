package com.cargill.spark.filesource

/**
 * Created by IntelliJ IDEA.
 *
 * @author : Chandan Balu
 * @created_date : 6/9/2020, Tue
 **/

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Class used to load a DataFrame from CSV file storage systems (e.g. file systems,
 * web URL, etc).
 */

class CSVFileReader(filePath: String, sparkSession: SparkSession) {

  var SOURCE_TYPE: String = "CSV"

  /**
   * Function to read the CSV dataset and returns the DataFrame object.
   * A CSV dataset is pointed to by filePath.
   * The path can be either a single text file or a directory storing text files
   * val path = "examples/src/main/resources/people.csv"
   */
  def getDataFrame(): DataFrame = {

    val sourceDataFrame = sparkSession
      .read
      .format("csv")
      .option("sep",";")
      .option("inferSchema","true")
      .option("header","true")
      .load(this.filePath)

    return sourceDataFrame
  }
}
