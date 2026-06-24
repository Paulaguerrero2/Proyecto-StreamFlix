package com.streamflix

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

object ETLProcessor {

  val logger: Logger = Logger.getLogger(getClass.getName)

  def run(
           rawLogsPath: String,
           moviesPath: String,
           outputPath: String
         )(implicit spark: SparkSession): Unit = {

    implicit val sc: SparkContext = spark.sparkContext

    try {

      // 1. LEER LOGS Y METADATOS

      logger.info("Leyendo logs")
      val rawLogsDF = leerLogs(rawLogsPath)

      rawLogsDF.show(5, false)

      logger.info("Leyendo películas")
      val rawMoviesDF = leerCSV(moviesPath)


      // 2. LIMPIAR Y PROCESAR LOGS

      logger.info("Procesando logs")
      val cleanLogsDF = procesarLogs(rawLogsDF)

      println("===== DEBUG CONTENT_ID =====")
      cleanLogsDF.select("content_id").show(20, false)
      println("===== FIN DEBUG =====")

      logger.info(s"Logs procesados: ${cleanLogsDF.count()}")


      // 3. ENRIQUECER DATOS (JOIN)

      logger.info("Enriqueciendo datos")
      val enrichedDF = enriquecerDatos(cleanLogsDF, rawMoviesDF)


      // 4. KPIs

      logger.info("Calculando KPIs")
      val kpisDF = calcularKPIs(enrichedDF)

      kpisDF.orderBy(desc("count")).show()

      val bingeDF = calcularBingeWatchers(enrichedDF)
      bingeDF.show()


      // 5. GUARDAR RESULTADOS

      logger.info("Guardando resultados")
      guardarResultados(kpisDF, outputPath)

      logger.info("Proceso ETL finalizado correctamente")

    } catch {
      case e: Exception =>
        logger.error("Error en el proceso ETL", e)
    }
  }


  // FUNCIONES DEL ETL

  // LEER LOGS
  def leerLogs(path: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.text(path)
  }

  // LEER CSV
  def leerCSV(path: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.option("header", "true").csv(path)
  }

  // PROCESAR LOGS
  def procesarLogs(logsDF: DataFrame)(implicit spark: SparkSession): DataFrame = {

    val filtrado = logsDF.filter(col("value").startsWith("[INFO]"))

    val transformado = filtrado
      .withColumn("timestamp",
        regexp_extract(col("value"), "(\\d{4}-\\d{2}-\\d{2} .*?)", 1))
      .withColumn("user_id",
        regexp_extract(col("value"), "User:(\\d+)", 1))
      .withColumn("content_id",
        regexp_extract(col("value"), "Movie_(\\d+)", 1))
      .filter(col("content_id") =!= "")
      .filter(col("user_id") =!= "")

    transformado
  }

  // ENRIQUECER DATOS
  def enriquecerDatos(
                       logsDF: DataFrame,
                       moviesDF: DataFrame
                     ): DataFrame = {


    val logsClean = logsDF
      .withColumn("content_id_clean", col("content_id").cast("double"))

    val moviesClean = moviesDF
      .withColumn("id_clean", col("id").cast("double"))

    logsClean.join(
      moviesClean,
      logsClean("content_id_clean") === moviesClean("id_clean"),
      "left"
    )
      .filter(col("genres").isNotNull)
  }

  // KPIs
  def calcularKPIs(enrichedDF: DataFrame): DataFrame = {

    enrichedDF.groupBy("genres").count()
  }

  // BINGE WATCHERS
  def calcularBingeWatchers(enrichedDF: DataFrame): DataFrame = {

    enrichedDF
      .groupBy("user_id")
      .agg(count("*").alias("views"))
      .filter(col("views") >= 5)
  }

  // GUARDAR RESULTADOS
  def guardarResultados(df: DataFrame, outputPath: String): Unit = {

    val finalDF = df
      .withColumn("year", lit(2025))
      .withColumn("country", lit("ES"))

    finalDF.write
      .mode("overwrite")
      .partitionBy("year", "country")
      .parquet(outputPath)
  }
}
