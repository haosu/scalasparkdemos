package haosu.sandbox.scalasparkdemos.RddVsDataset

import haosu.sandbox.scalasparkdemos.DemoBase
import org.apache.spark.sql.functions._

/**
  * We want to keep all people over the age of 30
  */
class FilterDemo extends DemoBase {
  // val peopleFilepath = getClass.getResource("/people.csv").toString
  val peopleFilepath = "/Users/hsu/Projects/scalasparkdemos/src/main/resources/people_no_header.csv"
  val headerPeopleFilename = "/Users/hsu/Projects/scalasparkdemos/src/main/resources/people_header.csv"

  def rddV1 = {
    val rdd = sc
      .textFile(peopleFilepath)
      .map { _.split(",") }
      .filter { row => row(2).toInt >= 30 }

    println(
      rdd
        .map { _.mkString("\t") }
        .collect()
        .mkString("\n")
    )
  }

  def datasetV1 = {
    val ds = spark
      .read
      .option("delimiter", ",")
      .csv(peopleFilepath)
      .where(col("_c2") >= 30)

    ds.show()
  }

  def datasetV2 = {
    val ds = spark
      .read
      .option("delimiter", ",")
      .option("header", "true")
      .csv(headerPeopleFilename)
      .where(col("age") >= 30)

    ds.show()
  }
}
