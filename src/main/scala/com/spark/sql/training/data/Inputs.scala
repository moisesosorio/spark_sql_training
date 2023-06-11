package com.spark.sql.training.data

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.spark.sql.training.config.Parameters
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

class Inputs extends Parameters {
  var sqlOperations_dataframe: DataFrame = _
  var arrayExample_dataframe: DataFrame = _

  def readInput(spark: SparkSession): Unit = {

    sqlOperations_dataframe = spark.read.format("csv")
      .option("inferschema", "true")
      .option("header", "true")
      .option("sep", ",")
      .load(inputPathSqlOperations)
  }

  def dataframeDummy(spark: SparkSession): Unit = {
    val array_structure_data = Seq(
      Row(Row("James", "", "Smith"), List("Java", "Scala", "C++"), "OH", "M"),
      Row(Row("Anna", "Rose", ""), List("Spark", "Java", "C++"), "NY", "F"),
      Row(Row("Julia", "", "Williams"), List("CSharp", "VB"), "OH", "F"),
      Row(Row("Maria", "Anne", "Jones"), List("CSharp", "VB"), "NY", "M"),
      Row(Row("Jen", "Mary", "Brown"), List("CSharp", "VB"), "NY", "M"),
      Row(Row("Mike", "Mary", "Williams"), List("Python", "VB"), "OH", "M")
    )

    val array_structure_schema = new StructType()
      .add("name", new StructType()
        .add("firstname", StringType)
        .add("middlename", StringType)
        .add("lastname", StringType))
      .add("languages", ArrayType(StringType))
      .add("state", StringType)
      .add("gender", StringType)

    arrayExample_dataframe = spark.createDataFrame(
      spark.sparkContext.parallelize(array_structure_data), array_structure_schema)

  }
}
