package com.core

import org.apache.log4j.{Level, Logger}
import java.util.Properties

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext

class Config {

  val Log = Logger.getLogger("Config")
  Log.setLevel(Level.DEBUG)
  var properties: Properties = new Properties()

  def loadConfig(configPath: String, sc : SparkContext) = {
    Log.info("Loading configuration from: " + configPath)
    val path = new Path(configPath)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val inputStream = fs.open(path)
    properties.load(inputStream)
    Log.info("Done loading configuration.")
  }

  def get(key: String): String = {
    if (properties == null)
      StringUtils.EMPTY
    else
      properties.getProperty(key)
  }

  def get(key: String, defValue: String): String = {
    if (properties == null)
      StringUtils.EMPTY
    val result = properties.getProperty(key)
    if (result == null)
      defValue
    else
      result
  }

}
