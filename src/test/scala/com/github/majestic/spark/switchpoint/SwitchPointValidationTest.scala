package com.github.majestic.spark.switchpoint

import com.github.majestic.spark.switchpoint.test.model.{DummyRecord, DummyRecordValidationResult}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class SwitchPointValidationTest extends AnyFlatSpec with should.Matchers {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val sparkTestSession = SparkSession
    .builder()
    .master("local[1]")
    .getOrCreate()

  import sparkTestSession.implicits._

  val testDataframe = DummyRecord.testSample.toDF

  val idRule = Rule("id")
    .isDefined
    .isUnique

  val field1Rule = Rule("field1")
    .isDefined
    .isIn(Seq(1))

  val field2Rule = Rule("field2")
    .isDefined
    .matchesPattern("o[a-z]+")

  val switchPointValidation = SwitchPointValidation()
    .addRule(idRule)
    .addRule(field1Rule)
    .addRule(field2Rule)

  "SwitchPointValidation" should "exclude bad records and keep good records" in {

    val result = switchPointValidation.runOn(testDataframe)

    val goodRecord = result.validDataFrame.collect()
    val badRecords = result.errorDataFrame
      .as[DummyRecordValidationResult]
      .collect()

    assert(goodRecord.size == 1, " - Only 1 good record should show up")
    assert(badRecords.size == 3, " - 3 bad records should show up")


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

    assert(recordValidationResult0.id_is_valid, " - Id for record0 should be valid")
    assert(!recordValidationResult0.field1_is_valid, " - field1 for record0 should be invalid")
    assert(recordValidationResult0.field2_is_valid, " - field2 for record0 should be valid")

    val recordValidationResult1 = badRecords.filter(_.id == "1").head

    assert(recordValidationResult1.id_is_valid, " - Id for record1 should be valid")
    assert(recordValidationResult1.field1_is_valid, " - field1 for record1 should be valid")
    assert(!recordValidationResult1.field2_is_valid, " - field2 for record1 should be invalid")

    val recordValidationResult2 = badRecords.filter(_.id == "2").head

    assert(recordValidationResult2.id_is_valid, " - Id for record2 should be valid")
    assert(recordValidationResult2.field1_is_valid, " - field1 for record2 should be valid")
    assert(!recordValidationResult2.field2_is_valid, " - field2 for record2 should be invalid")

  }



}
