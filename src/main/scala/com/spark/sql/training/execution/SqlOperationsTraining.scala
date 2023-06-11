package com.spark.sql.training.execution

import com.spark.sql.training.data.Inputs
import com.spark.sql.training.config.Parameters
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, row_number, lag, lead, rank, lit, ntile, avg, sum, min, max, count}
import org.apache.spark.sql.expressions.Window

object SqlOperationsTraining extends Inputs with Parameters {

  def windowsFunctions(config: Config, spark: SparkSession): Unit = {
    setVariablesParameter(config)
    readInput(spark)

    /**
     * windows_df => Dataframe with the columns that we need to demostrarte the result of the windows clause
     * windows_clause => Partition and ordering by columns to evaluate the menu, sugar and calories
     */
    val windows_df = sqlOperations_dataframe.select(col("menu"), col("sugar"), col("calories"))
    val window_clause = Window.partitionBy(col("menu"), col("sugar")).orderBy(col("calories"))

    /** Window Functions:
     * row_number => write the number per each different values by each partition
     * rank => ranking the values in the partition
     * ntile =>
     * lead => get the next column field value. Depends of the offset parameter. i.e. 1 => one next, 2 => two next, so on
     * lag => get the before column field value. Depends of the offset parameter. i.e. 1 => one before, 2 => 2 before, so on
     */

    val window_partition = windows_df
      .withColumn("row_number_col", lit(row_number().over(window_clause)))
      .withColumn("rank_col", lit(rank().over(window_clause)))
      .withColumn("ntile_col", ntile(3).over(window_clause))
      .withColumn("lead_col", lead(col("calories"),1).over(window_clause))
      .withColumn("lag_col", lag(col("calories"),1).over(window_clause))


    window_partition
      .filter(col("menu") === "mccafe")
      .show()

    /**
     * window_agg => partition by menu column and ordering by itself
     */
    val window_agg = Window.partitionBy("menu").orderBy("menu")

    /** Aggregation Functions:
     * avg => get the average value from all values in the partition
     * sum => get the sum of values in the partition
     * min => get the min value in the partition
     * max => get the max value in the partition
     */

    val window_partition_agg = windows_df.withColumn("row", row_number.over(window_agg))
      .withColumn("avg", avg(col("sugar")).over(window_agg))
      .withColumn("sum", sum(col("sugar")).over(window_agg))
      .withColumn("min", min(col("sugar")).over(window_agg))
      .withColumn("max", max(col("sugar")).over(window_agg))
      .withColumn("count", count(col("sugar")).over(window_agg))


    window_partition_agg
      .where(col("row") === 1)
      .select("menu", "avg", "sum", "min", "max", "count")
      .show()

  }

  def filterExamples(config: Config, spark: SparkSession): Unit = {
    setVariablesParameter(config)
    readInput(spark)

    /** Sintaxes for filter clause
     * => filter(condition: Column): Dataset[T]
     * => filter(conditionExpr: String): Dataset[T] -using SQL expression
     * => filter(func: T => Boolean): Dataset[T]
     * => filter(func: FilterFunction[T]): Dataset[T]
     *
     * Sign signature options:
     * => col("column_name")
     * => "column_name=='value'"
     * => df("column_name")
     */

    val filter_df = sqlOperations_dataframe
      .select(col("menu"), col("sugar"), col("calories"))

    filter_df.filter(col("menu") === "regular").show(false)
    filter_df.filter("menu=='regular'").show(false)
    filter_df.filter(filter_df("menu") === "regular").show(false)

    filter_df.where(col("menu") === "regular").show(false)
    filter_df.where("menu=='regular'").show(false)
    filter_df.where(filter_df("menu") === "regular").show(false)

    /**
     * Filter with conditional evaluations (and , or, lt, gt)
     */

    df.filter(col("menu") === "regular" && col("calories") === "M")
      .show(false)


    /**
     * Filter with negative operations (!)
     */


    /** Filter with more than one evaluation
     *
     */

  }

  def castingDataTypes(config: Config, spark: SparkSession): Unit = {
    setVariablesParameter(config)
    readInput(spark)

    /**
     * Cast from :
     * - string to int
     * - int to string
     * - date to string
     * - string to date
     * - string to timestamp
     * - timestamp to string
     * - string to double
     * - double to string
     * - string to list
     * - list to string
     *
     */




  }

  def forComprehension(config: Config, spark: SparkSession): Unit = {
    setVariablesParameter(config)

  }
}
