package com.spark.sql.training.config

import com.typesafe.config.Config

trait Parameters {
  var inputPathRDD: String = ""
  var inputPathSqlOperations: String = ""

  def setVariablesParameter(config: Config): Unit = {
    val paramsConfig = config.getConfig("paramLocal")
    inputPathRDD = paramsConfig.getString("inputPathRDD")
    inputPathSqlOperations = paramsConfig.getString("inputPathSqlOperations")

  }
}
