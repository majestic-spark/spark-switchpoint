package com.github.majestic.spark.switchpoint.test.model

case class  DummyRecord(id : String, field1 : Int, field2 : Option[String])

case class DummyRecordValidationResult(id : String, field1 : Int, field2 : Option[String], rule_id : Boolean,rule_on_field1 : Boolean, rule_on_field2 : Boolean, impossible_rule_with_where : Boolean, is_record_valid : Boolean)

object DummyRecord{

  val testSample = Seq(
    DummyRecord("0",0,Some("oops")),
    DummyRecord("1",1,None),
    DummyRecord("2",1,Some("bid")),
    DummyRecord("3",1,Some("oof")),
    DummyRecord("4",1,Some("oo"))
  )

}
