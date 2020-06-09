package com.cargill.spark.filesource

/**
 * Created by IntelliJ IDEA.
 *
 * @author : Chandan Balu
 * @created_date : 6/9/2020, Tue
 **/

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Class used to load a DataFrame from Parquet file storage systems (e.g. file systems,
 * web URL, etc).
 */

class ParquetFileReader(filePath: String, sparkSession: SparkSession) {

  var SOURCE_TYPE: String = "Parquet"

  /**
   * Function to read the Parquet dataset and returns the DataFrame object.
   * A Parquet dataset is pointed to by filePath.
   * Parquet files are self-describing so the schema is preserved
   * val path = "examples/src/main/resources/people.parquet"
   */
  def getDataFrame(): DataFrame = {

    val sourceDataFrame = sparkSession
      .read
      .load(this.filePath)

    return sourceDataFrame
  }
}
