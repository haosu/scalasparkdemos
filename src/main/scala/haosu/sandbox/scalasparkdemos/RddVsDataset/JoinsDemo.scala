package haosu.sandbox.scalasparkdemos.RddVsDataset

import haosu.sandbox.scalasparkdemos.DemoBase
import haosu.sandbox.scalasparkdemos.models.{CityRow, PersonRow}

class JoinsDemo extends DemoBase {
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

    val peoplePairRdd = peopleRdd.map { row => (row.city, row) }

    val citiesRdd = sc
      .textFile(citiesFilepath)
      .map { _.split(",") }
      .map { row =>
        CityRow(
          row(0),
          row(1).toInt,
          row(2),
          row(3)
        )
      }

    val citiesPairRdd = citiesRdd.map { row => (row.city, row) }

    val joinedRdd = peoplePairRdd.join(citiesPairRdd)

    println(
      joinedRdd.collect().mkString("\t")
    )
  }

  def datasetV1 = {
    val peopleDs = spark
      .read
      .json(jsonPeopleFilename)

    val citiesDs = spark
      .read
      .json(jsonCitiesFilename)

    peopleDs
      .join(
        citiesDs,
        "city"
      )
      .show()
  }
}
