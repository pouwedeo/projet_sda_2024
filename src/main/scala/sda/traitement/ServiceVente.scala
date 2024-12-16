package sda.traitement
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import java.time.LocalDate


object ServiceVente {

  implicit class DataFrameUtils(dataFrame: DataFrame) {

    def formatter()= {
      dataFrame.withColumn("HTT", split(col("HTT_TVA"), "\\|")(0))
               .withColumn("TVA", split(col("HTT_TVA"), "\\|")(1))
    }

    def calculTTC () : DataFrame ={

       dataFrame.withColumn("HTT", regexp_replace(col("HTT"), ",", "."))
                .withColumn("TVA", regexp_replace(col("TVA"), ",", "."))
                .withColumn("TTC", round(col("HTT").cast(DoubleType) + col("TVA").cast(DoubleType) * col("HTT").cast(DoubleType), 2))
                .drop("HTT", "TVA")


    }


    def extractDateEndContratVille(): DataFrame = {

        val schema_MetaTransaction = new StructType()
          .add("Ville", StringType, false)
          .add("Date_End_contrat", StringType, false)
        val schema = new StructType()
          .add("MetaTransaction", ArrayType(schema_MetaTransaction), true)

        val reg="[0-9]{4}-[0-9]{2}-[0-9]{2}"

      dataFrame.select(col("Id_Client"),col("HTT_TVA"),col("TTC"),from_json(col("MetaData"), schema).as("struct"))
                 .select(col("Id_Client"), col("HTT_TVA"), col("TTC"), explode(col("struct.MetaTransaction")).as("MetaTransaction"))
                 .withColumn("Date_End_contrat", regexp_extract(col("MetaTransaction.Date_End_contrat"), reg, 0))
                 .withColumn("Ville", col("MetaTransaction.Ville"))
                 .drop("MetaTransaction")
                 .select("*").na.drop("any")

    }

    def contratStatus(): DataFrame = {
        val today = LocalDate.now().toString

        val df = dataFrame.withColumn("Contrat_Status",when(col("Date_End_contrat") <= today, lit("Expired")).otherwise(lit("Actif"))  )
        df
    }


  }

}