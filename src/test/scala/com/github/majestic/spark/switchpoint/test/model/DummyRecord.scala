package com.github.majestic.spark.switchpoint.test.model

case class  DummyRecord(id : String, field1 : Int, field2 : Option[String])

case class DummyRecordValidationResult(id : String, field1 : Int, field2 : Option[String], id_is_valid : Boolean,field1_is_valid : Boolean,field2_is_valid : Boolean,is_record_valid : Boolean)

object DummyRecord{

  val testSample = Seq(
    DummyRecord("0",0,Some("oops")),
    DummyRecord("1",1,None),
    DummyRecord("2",1,Some("bid")),
    DummyRecord("3",1,Some("oof"))
  )

}
