package com.github.majestic.spark.switchpoint

import org.apache.spark.sql.Column

case class Constraint(constraintStatement : Column)

abstract class WhereConstraint

case class WhereColumnConstraint(columnStatement : Column) extends WhereConstraint

case class WhereSQLConstraint(sqlStatement : String) extends WhereConstraint
