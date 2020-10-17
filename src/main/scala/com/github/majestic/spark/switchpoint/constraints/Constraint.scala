package com.github.majestic.spark.switchpoint.constraints

import org.apache.spark.sql.Column

case class Constraint(constraintStatement : Column)

case class WhereConstraint(condition : Column)

