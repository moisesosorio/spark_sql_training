package com.spark.sql.training.execution

import com.spark.sql.training.data.Inputs
import com.spark.sql.training.config.Parameters
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, row_number, lag, lead, rank}

object SqlOperationsTraining extends Inputs with Parameters {

  def windowsFunctions(config: Config, spark: SparkSession): Unit = {
    setVariablesParameter(config)

    /** row_number
     * Use case: To put numbers by customiza partition
     */

    /** rank
     * Use case:
     */


    /** lag
     * Use case:
     */


    /** lead
     * Use case:
     */


    /** ntilde
     * Use case:
     */

    /** avg, sum, count, max, min
     * Use case:
     */


  }

  def filterExamples(config: Config, spark: SparkSession): Unit = {
    setVariablesParameter(config)
    readInput(spark)

    /** Simple filter
     *
     */

    val result_male_df = sqlOperations_dataframe
      .filter(col("gender") === "male")
      .select(col("name"), col("age"))
      .withColumn("age_final", col("age") + 1)
      .sort(col("age").desc, col("name").asc)

    //BUENAS PRACTICAS
    //filtrar --> para hacer el dataframa mas pequenio
    //seleccionar --> solo las columnas a utilizar
    //agregar la logica del negocio

    val result_female_df = sqlOperations_dataframe.filter(col("gender") === "female")
      .select(col("name"), col("age"))
      .withColumn("age_final", col("age") - 1)
      .sort(col("age").desc, col("name").asc)

    //BUENAS PRACTICA
    //partir los procesos para trabajo en paralelo
    val result_df = result_male_df.union(result_female_df)


  }

  def castingDataTypes(config: Config, spark: SparkSession): Unit = {
    setVariablesParameter(config)

  }

  def forComprehension(config: Config, spark: SparkSession): Unit = {
    setVariablesParameter(config)

  }
}
