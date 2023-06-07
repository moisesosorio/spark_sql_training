package com.spark.sql.training.execution

import com.spark.sql.training.data.Inputs
import com.spark.sql.training.config.Parameters
import com.typesafe.config.Config
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.rdd.{RDD}

object ResilientDistributedDataset extends Inputs with Parameters {

  case class FoodRDD(ingredient: String, quantity: Int)

  def rddTraining(config: Config, spark: SparkSession): Unit = {
    setVariablesParameter(config)
    basicOperations(spark)


  }

  def basicOperations(spark: SparkSession): Unit = {

    /** Declare an empty RDD
     * Use case: To create a single empty row to add this row inside the dataframe
     */
    val rddString = spark.sparkContext.emptyRDD[String]
    val rddFourPartitions = spark.sparkContext.parallelize(Seq.empty[String], 4)
    val num_of_partition = rddFourPartitions.getNumPartitions
    println(s"Number of partitions: $num_of_partition")

    /** Create a simple RDD
     * Use case: To prepare a POC or Demo or to create our unitary test
     */
    val columns = Seq("ingredient","quantity")
    val data = Seq(("Rise", 1000), ("Grapes", 800), ("Sugar", 100))
    val rddParallelize: RDD[(String, Int)] = spark.sparkContext.parallelize(data)

    val rdd = rddParallelize.map(i => FoodRDD(i._1, i._2))


    /** Mapping Operations over the result of read a file
     */
    val rddTextfile = spark.sparkContext.textFile(inputPathRDD)

    /** Use case: To read a file then analize the data with map
     * lineLengths = split each row by space then length
     * totalLength = reduce o put together all data and sum. (Shuffle)
     */
    val lineLengths = rddTextfile.map(s => s.split(' ').length)
    val totalLength = lineLengths.reduce((a, b) => a + b)
    println(s"Total words in the file: $totalLength")

    /** Use case: Change every letter in the file to upper case
     * collect = put together all values (Shuffle)
     */
    val uppercaseRDD = rddTextfile.map(word => word.toUpperCase())
    val result = uppercaseRDD.collect()
    result.foreach(println)
  }

}
