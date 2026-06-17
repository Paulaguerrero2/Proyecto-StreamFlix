package com.streamflix.processor

import org.apache.spark.sql.SparkSession

object Modulo2 {

  def iniciarModulo2()(implicit spark :SparkSession)= {

    println("=== MODULO 2: CARGA CSV ===")

    // Cargar csv

    val moviesDF = spark.read
      .option("header", "true")
      .csv("src/main/resources/data/movies_metadata.csv")

    moviesDF.show(10)


    println("=== MODULO 2: LIMPIAR PRECIO ===")

    import org.apache.spark.sql.functions._

    val cleanedDF = moviesDF.withColumn(
      "subscription_price",
      regexp_replace(col("subscription_price"), "\\$", "").cast("double")
    )

    cleanedDF.show(10)


    println("=== MODULO 2: LIMPIAR GENEROS ===")

    val genresCleanedDF = cleanedDF.withColumn(
      "genres",
      split(col("genres"), "\\|")
    )

    genresCleanedDF.show(10)



    println("=== MODULO 2: ANALISIS DE NULOS ===")

    genresCleanedDF.select(
      count(when(col("id").isNull, 1)).alias("id_nulls"),
      count(when(col("title").isNull, 1)).alias("title_nulls"),
      count(when(col("genres").isNull, 1)).alias("genres_nulls"),
      count(when(col("subscription_price").isNull, 1)).alias("price_nulls"),
      count(when(col("release_date").isNull, 1)).alias("date_nulls"),
      count(when(col("country").isNull, 1)).alias("country_nulls")
    ).show()


    println("=== MODULO 2: DUPLICADOS ===")

    val totalRows = genresCleanedDF.count()
    val distinctRows = genresCleanedDF.distinct().count()

    println("Total filas: " + totalRows)
    println("Filas sin duplicados: " + distinctRows)
    println("Duplicados: " + (totalRows - distinctRows))


    println("=== MODULO 2: MANEJO DE NULOS EN GENEROS ===")

    val finalDF = genresCleanedDF.withColumn(
      "genres",
      when(col("genres").isNull, array(lit("Unknown")))
        .otherwise(col("genres"))
    )

    finalDF.show(10)


    println("=== SCHEMA FINAL ===")
    finalDF.printSchema()

    println("=== DATOS LIMPIOS ===")
    finalDF.show(5)

    moviesDF
  }
}
