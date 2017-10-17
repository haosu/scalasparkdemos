package haosu.sandbox.scalasparkdemos

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait DemoBase {
  val sparkConf = new SparkConf().setMaster("local")
  val spark = new SparkSession(sparkConf)
  val sc = spark.sparkContext
}
