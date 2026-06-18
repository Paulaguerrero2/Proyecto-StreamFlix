
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

    implicit val sc: SparkContext = spark.sparkContext


    val filteredLogs = Modulo1.iniciarModulo1()
    val moviesDF = Modulo2.iniciarModulo2()
    val genreMetricsDF = Modulo3.iniciarModulo3()

    genreMetricsDF.show()

    val modulo4DF = Modulo4.iniciarModulo4()

    modulo4DF.show()

    Modulo5.iniciarModulo5(modulo4DF, moviesDF)

    spark.stop()

  }
}