package com.test

import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class WordCountTest extends AnyFunSuite with BeforeAndAfterAll { self =>

  var spark:SparkSession = _

  object testImplicits extends SQLImplicits with Serializable {
    protected override def _sqlContext: SQLContext = self.spark.sqlContext
  }

  import testImplicits._

  override protected def beforeAll(): Unit = {
    spark = SparkSession.builder().master("local[2]").getOrCreate()
  }

  test("Count logic is considering all records") {
    val seq = Seq("Akhil", "Naveen", "Vignesh", "Akhil")
    val rdd = WordCount.countWords(spark,seq.toDS())
    assert(rdd.collect().length === 3)
  }

  test("Count returned is correct") {
    val seq = Seq("Akhil", "Naveen", "Jacob", "Akhil")
    val rdd = WordCount.countWords(spark,seq.toDS())
    assert(rdd.collectAsMap().get("Akhil") === Some(2))
  }
}

