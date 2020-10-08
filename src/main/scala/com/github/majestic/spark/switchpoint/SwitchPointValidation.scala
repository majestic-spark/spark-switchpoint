package com.github.majestic.spark.switchpoint

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col


case class SwitchPointValidation(rules : Seq[Rule] = Seq.empty) {

  val topRuleName = "is_record_valid"

  def addRule(rule : Rule) = {
    SwitchPointValidation(
      rules :+ rule
    )
  }

  def runOn(dataFrame: DataFrame) : ValidationResult = {
      val dataframeWithAppliedRules = rules.foldLeft[DataFrame](dataFrame){
        case (df,rule) => {
          val compiledConstraints = Rule.compileConstraints(rule)
          df.withColumn(rule.getName,compiledConstraints)
        }
      }.withColumn(topRuleName, SwitchPointValidation.compileRules(rules))

    dataframeWithAppliedRules.cache()

    val validDataFrame = dataframeWithAppliedRules.filter(col(topRuleName))
    validDataFrame.cache()
    val errorDataFrame = dataframeWithAppliedRules.filter(!col(topRuleName))
    errorDataFrame.cache()

    dataframeWithAppliedRules.unpersist

    ValidationResult(validDataFrame,errorDataFrame)
  }


}

object SwitchPointValidation {

  def compileRules(rules : Seq[Rule]) : Column = {
    rules
      .map(rule => col(rule.name))
      .reduce(_ and _)
  }

}




