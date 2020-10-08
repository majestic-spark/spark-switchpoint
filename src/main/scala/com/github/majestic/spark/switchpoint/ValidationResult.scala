package com.github.majestic.spark.switchpoint

import org.apache.spark.sql.DataFrame

case class ValidationResult(validDataFrame : DataFrame, errorDataFrame : DataFrame){

  def freeUpCache : Unit = {
    validDataFrame.unpersist()
    errorDataFrame.unpersist()
  }

}

