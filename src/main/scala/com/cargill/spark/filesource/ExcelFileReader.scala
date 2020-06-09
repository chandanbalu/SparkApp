package com.cargill.spark.filesource

/**
 * Created by IntelliJ IDEA.
 *
 * @author : Chandan Balu
 * @created_date : 6/9/2020, Tue
 **/

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Class used to load a DataFrame from JSON file storage systems (e.g. file systems,
 * web URL, etc).
 */

class ExcelFileReader(filePath: String, sparkSession: SparkSession) {

  var SOURCE_TYPE: String = "Excel"

  /**
   * Function to read the JSON dataset and returns the DataFrame object.
   * A JSON dataset is pointed to by filePath.
   * The path can be either a single text file or a directory storing text files
   * val path = "examples/src/main/resources/people.json"
   */
  def getDataFrame(): DataFrame = {

    val sourceDataFrame = sparkSession
      .read
      .json(this.filePath)

    return sourceDataFrame
  }
}
