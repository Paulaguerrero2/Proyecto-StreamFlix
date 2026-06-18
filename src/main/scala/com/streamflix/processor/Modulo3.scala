
package com.streamflix.processor

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Modulo3 {

  def iniciarModulo3()(implicit spark: SparkSession): DataFrame = {

    println("=== MODULO 3: CARGAR DATOS ===")

    // Leer películas
    val moviesDF = spark.read
      .option("header", "true")
      .csv("src/main/resources/data/movies_metadata.csv")


    // Leer logs
    val logsDF_raw = spark.read
      .text("src/main/resources/data/server_logs.txt")



    println("=== MODULO 3: CREAR LOGS ===")

    // Crear logsDF

    val logsDF = logsDF_raw
      .withColumn("user_id",
        split(split(col("value"), "\\|")(0), ":")(1)
      )
      .withColumn("movie_id",
        split(split(col("value"), "\\|")(1), ":")(1)
      )
      .withColumn("duration_watched",
        split(split(col("value"), "\\|")(2), ":")(1).cast("int")
      )
      .select("user_id", "movie_id", "duration_watched")


    logsDF.show(10)



    println("=== MODULO 3: HACER JOIN ===")

    val enrichedDF = logsDF.join(
      moviesDF,
      logsDF("movie_id") === moviesDF("id"),
      "inner"
    )

    enrichedDF.show(10)



    println("=== MODULO 3: ANALISIS GENEROS ===")

    val genreMetricsDF = enrichedDF
      .withColumn("genres", split(col("genres"), "\\|"))
      .withColumn("genre", explode(col("genres")))
      .groupBy("genre")
      .agg(sum("duration_watched").alias("total_hours"))

    genreMetricsDF.show()

    genreMetricsDF
  }
}


