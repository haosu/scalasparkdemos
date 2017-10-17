package haosu.sandbox.scalasparkdemos.models

import org.apache.spark.sql.types._

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
