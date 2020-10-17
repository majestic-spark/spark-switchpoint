package com.github.majestic.spark.switchpoint

import com.github.majestic.spark.switchpoint.constraints.{Constraint, WhereConstraint}
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, sum}

case class Rule(ruleName : String, constraints : Seq[Constraint] = Seq.empty, whereConstraint : Option[WhereConstraint] = None) {

  def matchesPattern(fieldName : String, pattern : String) : Rule = {
    this.addConstraint(
        col(fieldName).rlike(pattern)
    )
  }

  def isIn(fieldName : String, lowerBound : Any, upperdBound : Any) : Rule = {
    this.addConstraint(
        col(fieldName).between(lowerBound,upperdBound)
    )
  }

  def isIn(fieldName : String, sequence : Seq[Any]) : Rule = {
    this.addConstraint(
        col(fieldName).isInCollection(sequence)
    )
  }

  def isDefined(fieldName : String) : Rule = {
    this.addConstraint(
      col(fieldName)
        .isNotNull
    )
  }

  def isNotEmpty(fieldName : String) : Rule = {
    this.addConstraint(
      col(fieldName)
        .notEqual(lit(""))
    )
  }

  def isUnique(fieldName : String) : Rule = {
    val one = lit(1)
    val window = Window
      .partitionBy(col(fieldName))

    this.addConstraint(
        sum(one)
          .over(window)
          .equalTo(one)
    )
  }

  def addConstraint(constraintColumn : Column): Rule = {
    val constraint = Constraint(constraintColumn)
    Rule(ruleName,
      constraints :+ constraint,
      whereConstraint
    )
  }

  def where(condition : Column) : Rule = {
    Rule(ruleName,constraints,Some(WhereConstraint(condition)))
  }

}

object Rule {

  def compileConstraints(rule : Rule) : Column = {
    rule
      .constraints
      .map(_.constraintStatement)
      .reduce(_ and _ )
  }


}