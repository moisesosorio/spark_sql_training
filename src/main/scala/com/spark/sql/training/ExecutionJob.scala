package com.spark.sql.training

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import com.spark.sql.training.execution.{ResilientDistributedDataset => rd}

protected trait ExecutionJobTrait {
  def main(args: Array[String]): Unit = {
    val reference = args(0)
    val globalConfig = ConfigFactory.load(reference)
    val configIn = globalConfig.getConfig("ScalaSparkTraining")

    val spark = SparkSession
      .builder()
      .appName("Spark SQL Training")
      .master("local")
      .getOrCreate()

    val executionTraining = configIn.getString("executionTraining")

//    rd.rddTraining(configIn, spark)

    executionTraining match {
      case "1" => rd.rddTraining(configIn, spark)
      case _ => "Anything to execute"

    }
  }
}

object ExecutionJob extends ExecutionJobTrait
