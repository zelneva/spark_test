import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions._


object DfJob {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("DfJob")
      .config("spark.master", "local")
      .getOrCreate()

    import sparkSession.implicits._

    val df1 = loadFile(Files.FILE_1, sparkSession)
    val filterData1 = filterData(df1)
    val data1 = loadData(filterData1)

    val df2 = loadFile(Files.FILE_2, sparkSession)
    val filterData2 = filterData(df2)
    val data2 = loadData(filterData2)


    val countryYearWindow: WindowSpec = Window.partitionBy('country).orderBy('year.desc)
    val countryPopulationWindow = Window.partitionBy('country).orderBy('populationCity.desc)


    val countryPopulation = getPopulationForLastYears(data1, countryYearWindow)
//    countryPopulation.rdd.saveAsTextFile(Files.outputDFPopulation)

    val countCitiesMillion = getCountCitiesMillon(data1, countryYearWindow)


    val top5cities = getTop5Cities(data1, countryYearWindow, countryPopulationWindow)


  }


  def loadFile(path: String, sparkSession: SparkSession) = {
    sparkSession.read.format("csv").option("header", "true").load(path)
  }

  def filterData(dataFrame: DataFrame) = {
    dataFrame.filter(row => row.length > 9 && row(9) != null && !row(9).toString.matches("[a-zA-Z]*"))
  }


  def loadData(data: DataFrame): DataFrame = {
    data.map(row => Population(row.getString(0),
      row(1).toString.toInt,
      row(4).toString,
      row(9).toString.toDouble,
      row(3).toString))(Encoders.product).toDF()
  }


  def getPopulationForLastYears(data: DataFrame, maxYear: WindowSpec): DataFrame = {
    data
      .select("country", "year", "population")
      .withColumn("max_year", max("year").over(maxYear))
      .where("year = max_year")
      .groupBy("country")
      .agg(sum("population").as("populationForLastYear"))
  }


  def getCountCitiesMillon(data: DataFrame,  maxYear: WindowSpec): DataFrame = {
    data
    .select("country", "city", "year", "population")
      .withColumn("max_year", max("year").over(maxYear))
      .where("year = max_year")
      .groupBy("country", "city")
      .agg(sum("population").as("populationForLastYear"))
      .filter("populationForLastYear >= 1000000")
      .groupBy("country")
      .agg(count("city"))
  }


  def getTop5Cities(data: DataFrame, maxYear: WindowSpec, countryPopulationWindow: WindowSpec ): DataFrame ={
    data
    .select("country", "city", "year", "population")
      .withColumn("max_year", max("year").over(maxYear))
      .where("year = max_year")
      .groupBy("country", "city")
      .agg(sum("population").as("populationCity"))
      .withColumn("row", row_number().over(countryPopulationWindow))
      .orderBy( data("country"), desc("populationCity"))
      .where("row <= 5")
      .drop("row", "populationCity")
  }


}
