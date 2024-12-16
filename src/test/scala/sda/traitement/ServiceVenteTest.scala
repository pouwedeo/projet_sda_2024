package sda.traitement

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ServiceVenteTest extends AnyFunSuite with Matchers {

  implicit val spark: SparkSession = SparkSession
    .builder
    .appName("SDA")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._
  import ServiceVente.DataFrameUtils

  test("Test formatter") {
    val input = Seq(
      ("1", "100,5|0,19"),
      ("2", "200,7|0,20")
    ).toDF("Id_Client", "HTT_TVA")

    val expected = Seq(
      ("1", "100,5|0,19", "100,5", "0,19"),
      ("2", "200,7|0,20", "200,7", "0,20")
    ).toDF("Id_Client", "HTT_TVA", "HTT", "TVA")

    val result = input.formatter()

    assert(result.collect() === expected.collect())
  }

  test("Test calculTTC") {
    val input = Seq(
      ("1", "100.5", "0.19"),
      ("2", "200.7", "0.20")
    ).toDF("Id_Client", "HTT", "TVA")

    val expected = Seq(
      ("1", 119.6),
      ("2", 240.84)
    ).toDF("Id_Client", "TTC")

    val result = input.calculTTC()

    assert(result.collect() === expected.collect())
  }

  test("Test extractDateEndContratVille") {
    val input = Seq(
      ("1", "100,5|0,19", 119.6, "{\"MetaTransaction\":[{\"Ville\":\"Paris\",\"Date_End_contrat\":\"2024-12-23\"}]}"),
      ("2", "200,7|0,20", 240.84, "{\"MetaTransaction\":[{\"Ville\":\"Alger\",\"Date_End_contrat\":\"2023-12-23\"}]}")
    ).toDF("Id_Client", "HTT_TVA", "TTC", "MetaData")

    val expected = Seq(
      ("1", "100,5|0,19", 119.6, "2024-12-23", "Paris"),
      ("2", "200,7|0,20", 240.84, "2023-12-23", "Alger")
    ).toDF("Id_Client", "HTT_TVA", "TTC", "Date_End_contrat", "Ville")

    val result = input.extractDateEndContratVille()

    assert(result.collect() === expected.collect())
  }

  test("Test contratStatus") {
    val input = Seq(
      ("1", "100,5|0,19", 119.6, "2024-12-23", "Paris"),
      ("2", "200,7|0,20", 240.84, "2023-12-23", "Alger")
    ).toDF("Id_Client", "HTT_TVA", "TTC", "Date_End_contrat", "Ville")

    val today = "2023-12-16"

    val expected = Seq(
      ("1", "100,5|0,19", 119.6, "2024-12-23", "Paris", "Actif"),
      ("2", "200,7|0,20", 240.84, "2023-12-23", "Alger", "Expired")
    ).toDF("Id_Client", "HTT_TVA", "TTC", "Date_End_contrat", "Ville", "Contrat_Status")

    val result = input.contratStatus()

    assert(result.collect() === expected.collect())
  }

}
