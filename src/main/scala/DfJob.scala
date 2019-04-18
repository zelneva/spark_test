import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession, functions}
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
    val a = filterData(df1)
    val b = loadData(a)


    val maxYear: WindowSpec = Window.partitionBy('country).orderBy('year.desc)


    val countryPopulation = getPopulationForLastYears(b, maxYear)
    //countryPopulation.rdd.map(_.toString().replace("[","").replace("]", "")).saveAsTextFile(Files.outputDFPopulation)

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


}
