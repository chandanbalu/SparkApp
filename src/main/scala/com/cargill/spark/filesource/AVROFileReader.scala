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
 * The spark-avro module is external and not included in spark-submit
 * ./bin/spark-submit --packages org.apache.spark:spark-avro_2.12:2.4.5 ...
 */

class AVROFileReader(filePath: String, sparkSession: SparkSession) {

  var SOURCE_TYPE: String = "AVRO"

  /**
   * Function to read the AVRO dataset and returns the DataFrame object.
   * A AVRO dataset is pointed to by filePath.
   * The path can be either a single text file or a directory storing text files
   * val path = "examples/src/main/resources/users.avro"
   */
  def getDataFrame(): DataFrame = {

    val sourceDataFrame = sparkSession
      .read
      .format("avro")
      .load(this.filePath)

    return sourceDataFrame
  }
}
