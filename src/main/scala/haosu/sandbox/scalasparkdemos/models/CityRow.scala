package haosu.sandbox.scalasparkdemos.models

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

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
