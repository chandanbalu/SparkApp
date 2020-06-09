package com.cargill.spark.filesource

/**
 * Created by IntelliJ IDEA.
 *
 * @author : Chandan Balu
 * @created_date : 6/9/2020, Tue
 **/

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Class used to load a DataFrame from ORC file storage systems (e.g. file systems,
 * web URL, etc).
 * Spark’s ORC data source supports complex data types (i.e., array, map, and struct),
 * and provides read and write access to ORC files.
 */

class ORCFileReader(filePath: String, sparkSession: SparkSession) {

  var SOURCE_TYPE: String = "ORC"

  /**
   * Function to read the ORC dataset and returns the DataFrame object.
   * A ORC dataset is pointed to by filePath.
   * The path can be either a single text file or a directory storing text files
   * val path = "examples/src/main/resources/people.orc"
   */
  def getDataFrame(): DataFrame = {

    val sourceDataFrame = sparkSession
      .read
      .format("orc")
      .load(this.filePath)

    return sourceDataFrame
  }
}
