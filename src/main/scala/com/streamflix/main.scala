package com.streamflix
import org.apache.spark.SparkContext
import processor.Modulo1
import processor.Modulo2
import org.apache.spark.sql.SparkSession
import processor.Modulo3
import processor.Modulo4
import processor.Modulo5

object Main {
  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("StreamFlix Analytics")
      .master("local[*]")
      .getOrCreate()


    val logsPath = args(0)
    val moviesPath = args(1)
    val outputPath = args(2)


    ETLProcessor.run(
      Config.logsPath,
      Config.moviesPath,
      Config.outputPath
    )

    spark.stop()
  }
}
