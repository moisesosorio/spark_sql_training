package com.spark.sql.training.data

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.spark.sql.training.config.Parameters

class Inputs extends Parameters {
  var sqlOperations_dataframe: DataFrame = _

  def readInput(spark: SparkSession): Unit = {

    sqlOperations_dataframe = spark.read.format("csv")
      .option("inferschema", "true")
      .option("header", "true")
      .option("sep", ",")
      .load(inputPathSqlOperations)
  }
}
