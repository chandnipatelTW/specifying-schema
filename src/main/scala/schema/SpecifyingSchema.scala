package schema

import org.apache.spark.sql.SparkSession

object SpecifyingSchema {
  val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("pass in schema")
      .getOrCreate()
  }
  val fileName = "./src/test/resources/clickstream.csv"

  def setNoSchema(): String = {
    spark.read.option("header", "true")
      .csv(fileName).schema.toString()
  }

  def setInferSchema(): String = {
    spark.read.option("header", "true")
      .csv(fileName).schema.toString()
    ???

  }

  def setExplicitSchema(): String = {
    spark.read.option("header", "true")
      .csv(fileName).schema.toString()
    ???
  }

  def setMissingFieldSchema(): String = {
    spark.read.option("header", "true")
      .csv(fileName).schema.toString()
    ???
  }

  def setErrorSchema(): String = {
    spark.read.option("header", "true")
      .csv(fileName).schema.toString()
    ???
  }
}
