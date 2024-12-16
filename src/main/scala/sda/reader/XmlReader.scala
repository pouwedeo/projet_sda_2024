package sda.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

case class XmlReader(path: String, rowTag: Option[String] = None) extends Reader {

  val format = "xml"

  def read()(implicit  spark: SparkSession): DataFrame = {

    spark.read.format(format)
      .option("rowTag", rowTag.getOrElse("Client"))
      .load(path)

  }
}



