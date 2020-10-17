package com.github.majestic.spark.switchpoint

import com.github.majestic.spark.switchpoint.test.model.{DummyRecord, DummyRecordValidationResult}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class SwitchPointValidationTest extends AnyFlatSpec with should.Matchers {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val sparkTestSession = SparkSession
    .builder()
    .master("local[1]")
    .getOrCreate()

  import sparkTestSession.implicits._

  val testDataframe = DummyRecord.testSample.toDF

  val idRule = Rule("rule_id")
    .isDefined("id")
    .isUnique("id")

  val field1Rule = Rule("rule_on_field1")
    .isDefined("field1")
    .isIn("field1", Seq(1))

  val field2Rule = Rule("rule_on_field2")
    .isDefined("field2")
    .matchesPattern("field2", "o[a-z]+")

  val ruleWithWhereStatement = Rule("impossible_rule_with_where")
    .isIn("id",Seq("0"))
    .where(col("id").equalTo(lit("4")))

  val switchPointValidation = SwitchPointValidation()
    .addRule(idRule)
    .addRule(field1Rule)
    .addRule(field2Rule)
    .addRule(ruleWithWhereStatement)


  "SwitchPointValidation.runOn()" should "exclude bad records and keep good records" in {


    val result = switchPointValidation.runOn(testDataframe)

    val goodRecord = result.validDataFrame.collect()
    val badRecords = result.errorDataFrame
      .as[DummyRecordValidationResult]
      .collect()

    assert(goodRecord.length == 1, " - Only 1 good record should show up")
    assert(badRecords.length == 4, " - 3 bad records should show up")


    /*
    Expecting :
    +---+------+------+-----------+---------------+---------------+---------------+
    | id|field1|field2|id_is_valid|field1_is_valid|field2_is_valid|is_record_valid|
    +---+------+------+-----------+---------------+---------------+---------------+
    |  0|     0|  oops|       true|          false|           true|          false|
    |  1|     1|  null|       true|           true|          false|          false|
    |  2|     1|   bid|       true|           true|          false|          false|
    +---+------+------+-----------+---------------+---------------+---------------+
     */

    assert(badRecords.forall(!_.is_record_valid), " - All bad records should be flagged as invalid")

    val recordValidationResult0 = badRecords.filter(_.id == "0").head

    assert(recordValidationResult0.rule_id, " - rule_id for record0 should be valid")
    assert(!recordValidationResult0.rule_on_field1, " - rule_on_field1 for record0 should be invalid")
    assert(recordValidationResult0.rule_on_field2, " - rule_on_field2 for record0 should be valid")
    assert(recordValidationResult0.impossible_rule_with_where, " - impossible_rule_with_where for record0 should be valid")

    val recordValidationResult1 = badRecords.filter(_.id == "1").head

    assert(recordValidationResult1.rule_id, " - rule_id for record1 should be valid")
    assert(recordValidationResult1.rule_on_field1, " - rule_on_field1 for record1 should be valid")
    assert(!recordValidationResult1.rule_on_field2, " - rule_on_field2 for record1 should be invalid")
    assert(recordValidationResult1.impossible_rule_with_where, " - impossible_rule_with_where for record1 should be valid")

    val recordValidationResult2 = badRecords.filter(_.id == "2").head

    assert(recordValidationResult2.rule_id, " - rule_id for record2 should be valid")
    assert(recordValidationResult2.rule_on_field1, " - rule_on_field1 for record2 should be valid")
    assert(!recordValidationResult2.rule_on_field2, " - rule_on_field2 for record2 should be invalid")
    assert(recordValidationResult2.impossible_rule_with_where, " - impossible_rule_with_where for record2 should be valid")

    val recordValidationResult4 = badRecords.filter(_.id == "4").head

    assert(recordValidationResult4.rule_id, " - rule_id for record4 should be valid")
    assert(recordValidationResult4.rule_on_field1, " - rule_on_field1 for record4 should be valid")
    assert(recordValidationResult4.rule_on_field2, " - rule_on_field2 for record4 should be invalid")
    assert(!recordValidationResult4.impossible_rule_with_where, " - impossible_rule_with_where for record4 should be invalid")

  }

}
