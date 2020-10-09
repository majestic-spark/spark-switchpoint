package com.github.majestic.spark.switchpoint

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, sum}

case class Rule(fieldName : String, constraints : Seq[Constraint] = Seq.empty, whereConstraint : Option[WhereConstraint] = None) {

  val name = fieldName+"_is_valid"
  val field = col(fieldName)


  def matchesPattern(pattern : String) : Rule = {
    this.addConstraint(
        field.rlike(pattern)
    )
  }

  def isIn(lowerBound : Any, upperdBound : Any) : Rule = {
    this.addConstraint(
        field.between(lowerBound,upperdBound)
    )
  }

  def isIn(sequence : Seq[Any]) : Rule = {
    this.addConstraint(
        field.isInCollection(sequence)
    )
  }

  def isDefined : Rule = {
    this.addConstraint(
      field
        .isNotNull
    )
  }

  def isNotEmpty : Rule = {
    this.addConstraint(
      field
        .notEqual(lit(""))
    )
  }

  def isUnique : Rule = {
    val one = lit(1)
    val window = Window
      .partitionBy(field)

    this.addConstraint(
        sum(one)
          .over(window)
          .equalTo(one)
    )
  }

  def addConstraint(constraintColumn : Column): Rule = {
    val constraint = Constraint(constraintColumn)
    Rule(fieldName,
      constraints :+ constraint,
      whereConstraint
    )
  }

  def getName : String = {
    fieldName+"_is_valid"
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