package haosu.sandbox.scalasparkdemos.RddVsDataset

import haosu.sandbox.scalasparkdemos.DemoBase
import haosu.sandbox.scalasparkdemos.models.PersonRow
import org.apache.spark.sql.SaveMode
import org.json4s.jackson.JsonMethods

class ReadingWritingDemo extends DemoBase {
  val peopleFilepath = "/Users/hsu/Projects/scalasparkdemos/src/main/resources/people_no_header.csv"
  val headerPeopleFilename = "/Users/hsu/Projects/scalasparkdemos/src/main/resources/people_header.csv"
  val jsonPeopleFilename = "/Users/hsu/Projects/scalasparkdemos/src/main/resources/people.json"

  val outputDir = "/Users/hsu/Projects/scalasparkdemos/output"

  def rddV1 = {
    val rdd = sc
      .textFile(peopleFilepath)
      .map { _.split(",") }

    rdd
      .map { _.mkString(",")}
      .saveAsTextFile(s"${outputDir}/readingwriting/rddv1.csv")
  }

  def rddV2 = {
    val rdd = sc
      .textFile(peopleFilepath)
      .map { _.split(",") }

    val header = rdd.take(1)

    val noHeaderRdd = rdd
      .filter { _ != header }
  }

  def rddJson = {
    val rdd = sc
      .textFile(jsonPeopleFilename)
      .map { JsonMethods.parse(_) }
  }

  def datasetV1 = {
    val ds = spark
      .read
      .schema(PersonRow.Schema)
      .option("delimiter", ",")
      .csv(peopleFilepath)

    ds
      .write
      .mode(SaveMode.Overwrite)
      .csv(s"${outputDir}/readingwriting/datasetv1/multipartition")

    ds
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .csv(s"${outputDir}/readingwriting/datasetv1/singlepartition")

    ds
      .write
      .mode(SaveMode.Overwrite)
      .json(s"${outputDir}/readingwriting/datasetv1/json")
  }

  def datasetV2 = {
    val ds = spark
      .read
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(headerPeopleFilename)

    ds
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(s"${outputDir}/readingwriting/datasetv2/")
  }
}
