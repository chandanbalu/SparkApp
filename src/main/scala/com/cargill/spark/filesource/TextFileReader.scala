package com.cargill.spark.filesource

/**
 * Created by IntelliJ IDEA.
 *
 * @author : Chandan Balu
 * @created_date : 6/9/2020, Tue
 **/

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, Row}

/**
 * Class used to load a DataFrame from TEXT file storage systems (e.g. file systems,
 * web URL, etc).
 */

class TextFileReader(filePath: String, sparkSession: SparkSession) {

  var SOURCE_TYPE: String = "TEXT"


  def getSchema(fileHeader: String): StructType ={

    // Defining schema from header which we defined above
    // It uses library org.apache.spark.sql.types.{StructType, StructField, StringType}
    val schema = StructType(
      fileHeader.split(" ")
        .map(fieldName => StructField(fieldName,StringType, true))
    )
    return schema
  }


  /**
   * Function to read the TEXT dataset and returns the DataFrame object.
   * A TEXT dataset is pointed to by filePath.
   * The path can be either a single text file or a directory storing text files
   * val path = "examples/src/main/resources/people.txt"
   */
  def getDataFrame(): DataFrame = {


    // Defining the data-frame header structure
    val fileHeader = "time duration client_add result_code bytes req_method url user hierarchy_code type"
    val dataSchema = getSchema(fileHeader)

    // Create an RDD of Person objects from a text file, convert it to a Dataframe
    val sourceRDD = sparkSession
      .sparkContext
      .textFile(this.filePath)

    // Converting String RDD to Row RDD for 10 attributes
    val rowRDD = sourceRDD
      .map(_.split(" "))
      .map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5) , x(6) , x(7) , x(8), x(9)))

    // Creating data-frame based on Row RDD and schema
    val sourceDataFrame = sparkSession.createDataFrame(rowRDD, dataSchema)

    return sourceDataFrame
  }
}
