package haosu.sandbox.scalasparkdemos

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.json4s.jackson.JsonMethods

class DemoEnvSetup {
  val peopleFilepath = "/Users/hsu/Projects/scalasparkdemos/src/main/resources/people_no_header.csv"
  val citiesFilepath = "/Users/hsu/Projects/scalasparkdemos/src/main/resources/cities_no_header.csv"
  val headerPeopleFilename = "/Users/hsu/Projects/scalasparkdemos/src/main/resources/people_header.csv"
  val jsonPeopleFilename = "/Users/hsu/Projects/scalasparkdemos/src/main/resources/people.json"
  val jsonCitiesFilename = "/Users/hsu/Projects/scalasparkdemos/src/main/resources/cities.json"

  val outputDir = "/Users/hsu/Projects/scalasparkdemos/output"

  case class PersonRow(
    id: BigInt,
    name: String,
    age: BigInt,
    city: String,
    last_seen_at: String
  )

  object PersonRow {
    val Schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("city", StringType),
      StructField("last_seen_at", StringType)
    ))
  }

  case class CityRow(
    city: String,
    population: Int,
    id: String,
    city_seal: String
  )

  object CityRow {
    val Schema = StructType(Seq(
      StructField("city", StringType),
      StructField("population", IntegerType),
      StructField("id", StringType),
      StructField("city_seal", StringType)
    ))
  }
}
