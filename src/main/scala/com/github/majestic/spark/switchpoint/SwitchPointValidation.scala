package com.github.majestic.spark.switchpoint

import com.github.majestic.spark.switchpoint.SwitchPointValidation.applyRule
import com.github.majestic.spark.switchpoint.constraints.WhereConstraint
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}


case class SwitchPointValidation(rules : Seq[Rule] = Seq.empty) {

  val topRuleName = "is_record_valid"

  def addRule(rule : Rule) = {
    SwitchPointValidation(
      rules :+ rule
    )
  }

  def runOn(dataFrame: DataFrame) : ValidationResult = {
      val dataframeWithAppliedRules = rules
        .foldLeft[DataFrame](dataFrame)(applyRule)
        .withColumn(topRuleName, SwitchPointValidation.compileRules(rules))

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
      .map(rule => col(rule.ruleName))
      .reduce(_ and _)
  }

  def applyRule(df : DataFrame, rule : Rule) : DataFrame = {
    val compiledConstraints = Rule.compileConstraints(rule)

    rule.whereConstraint match {
      case None => df.withColumn(rule.ruleName,compiledConstraints)
      case Some(WhereConstraint(condition)) => {
        df.withColumn(rule.ruleName, when(condition,compiledConstraints).otherwise(lit(true)))
      }
    }

  }

}




