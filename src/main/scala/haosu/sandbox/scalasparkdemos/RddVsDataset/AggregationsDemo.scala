package haosu.sandbox.scalasparkdemos.RddVsDataset

import haosu.sandbox.scalasparkdemos.DemoBase
import haosu.sandbox.scalasparkdemos.models.PersonRow
import org.apache.spark.sql.functions._

class AggregationsDemo extends DemoBase {
  val peopleFilepath = "/Users/hsu/Projects/scalasparkdemos/src/main/resources/people_no_header.csv"
  val citiesFilepath = "/Users/hsu/Projects/scalasparkdemos/src/main/resources/cities_no_header.csv"
  val jsonPeopleFilename = "/Users/hsu/Projects/scalasparkdemos/src/main/resources/people.json"
  val jsonCitiesFilename = "/Users/hsu/Projects/scalasparkdemos/src/main/resources/cities.json"

  def rddV1 = {
    val peopleRdd = sc
      .textFile(peopleFilepath)
      .map { _.split(",") }
      .map { row =>
        PersonRow(
          row(0).toInt,
          row(1),
          row(2).toInt,
          row(3),
          row(4)
        )
      }

    peopleRdd
      .groupBy { _.city }
      .map { case (city, rows) =>
        val residentCount = rows.size
        val oldestPerson = rows.maxBy { _.age }
        (city, residentCount, oldestPerson.age)
      }
  }

  def datasetV1 = {
    val peopleDs = spark
      .read
      .json(jsonPeopleFilename)

    peopleDs
      .groupBy("city")
      .agg(
        count("name"),
        max("age")
      )
  }
}
