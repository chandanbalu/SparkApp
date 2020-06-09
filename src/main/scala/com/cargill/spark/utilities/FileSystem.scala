package com.cargill.spark.utilities

/**
 * Created by IntelliJ IDEA.
 *
 * @author : Chandan Balu
 * @created_date : 6/9/2020, Tue
 **/
class FileSystem {

  // Function to get the file path based on the OS name
  def getFilePath(fileName: String): String ={

    var resourcePath: String = null
    val osName: String = System.getProperty("os.name").toString

    // Set the path based on the OS name
    osName.startsWith("Windows") match {
      case true =>
        resourcePath = System.getProperty("user.dir").toString + "\\" + fileName
      case false =>
        resourcePath = System.getProperty("user.dir").toString + "/" + fileName
    }
    println("Setting the resourcePath:" + resourcePath)
    return resourcePath
  }

}
