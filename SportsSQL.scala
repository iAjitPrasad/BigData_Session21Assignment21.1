package SQL

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf

object SportsSQL {

  case class SportsDetails(FName: String, LName: String, Sports: String, MedalType: String, sAge:Long, Year: Long, Nationality: String)

  def Ranking(Age: Int, Medal: String): String = {
    if(Medal == "gold" && Age >= 32) "Pro"
    else if(Medal == "gold" && Age <= 31) "Amateur"
    else if(Medal == "silver" && Age >= 32)"Expert"
    else if(Medal == "silver" && Age <= 31)"Rookie"
    else ""
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    /***Set the log level as warning***/
    spark.sparkContext.setLogLevel("WARN")

    println("Spark Session Object Created for Sports Data Analysis")

    val sportsData = spark.sparkContext
      .textFile("D:\\AcadGild\\ScalaCaseStudies\\Datasets\\Sports\\s21\\SportsDataset.txt")

    val header = sportsData.first()
    val newData = sportsData.filter(row => row != header)

    println("\nHeader removed from the data!")

    /***For implicit conversions like converting RDDs and sequences to DataFrames***/
    import spark.implicits._

    /***Loading Sports Dataset***/
    val sportsDF = newData
      .map(x => x.split(","))
      .map(x => SportsDetails(x(0), x(1), x(2), x(3), x(4).trim.toInt, x(5).trim.toInt, x(6)))
      .toDF()
    print("\nSports dataframe displayed\n")
    sportsDF.show()

    /***What are the total number of gold medal winners every year?***/
    println("\nList of Gold medal winners every year")
    sportsDF.filter(col("MedalType") === "gold")
      .show()

    val medal = sportsDF.filter(col("MedalType") === "gold")
    println("Total number of Gold medal winners every year: " + medal.count())

    /***How many silver medals have been won by USA in each sports?***/
    println("\n\nList of Silver medal winners from USA")
    sportsDF.filter((col("MedalType") === "silver") && col("Nationality") === "USA")
      .show()

    val silverMedals = sportsDF
      .filter((col("MedalType") === "silver") && col("Nationality") === "USA")
    println("Total number Silver medals won by USA in each sport: " + silverMedals.count())


    /***Change firstname, lastname columns into
      Mr.first_two_letters_of_firstname<space>lastname
      for example - michael, phelps becomes Mr.mi phelps***/

    println("\n\nUsing udfs on dataframe")
    val Name = udf((FName: String, LName: String) => "Mr. "
      .concat(FName.substring(0, 2).concat(" ")
        .concat(LName)))

    println("\nDisplaying list by concatenating names")
    sportsDF.registerTempTable("SportsData")
    sportsDF.withColumn("FNameConcatLName", Name($"FName", $"LName"))
      .select("FNameConcatLName", "Sports", "MedalType", "sAge", "Year", "Nationality")
      .show()

    /***Add a new column called Ranking using UDFs on dataframe defined under Ranking function***/
    val Rank = udf(Ranking(_: Int, _: String))
    println("\nRanking the players accordingly")
    sportsDF.withColumn("Ranks", Rank($"sAge", $"MedalType"))
      .select("Ranks", "FName", "LName", "sAge", "MedalType")
      .show()
  }
}
