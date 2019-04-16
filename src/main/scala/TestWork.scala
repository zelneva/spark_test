import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}


object TestWork {

  object Files {
    val FILE_1 = "C:\\Users\\Администратор\\Downloads\\Telegram Desktop\\unsd-citypopulation-year-both.csv"
    val FILE_2 = "C:\\Users\\Администратор\\Downloads\\Telegram Desktop\\unsd-citypopulation-year-fm.csv"
  }


  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("TestWork")
      .config("spark.master", "local")
      .getOrCreate()

    val input1 = loadFile(Files.FILE_1, sparkSession)
    val data1 = filterData(input1)
    val population1 = loadData(data1)

    val data2 = loadFile(Files.FILE_2, sparkSession)


    // подсчет населения стран

    val countryYear = countPopulationForAllYears(population1)
    val populationLastYear = countPopulationForLastYear(countryYear)

    val outputFile = "result.txt"
    populationLastYear.saveAsTextFile(outputFile)


    //подсчет городов миллионников

    val countryWithCityMillion = countPopulationCity(population1)
    val allCountryCountCityMillion = countCityMillionInEveryCountry(data1, countryWithCityMillion)

    val outputCityMillion = "outputMillion.txt"
    allCountryCountCityMillion.saveAsTextFile(outputCityMillion)
  }


  def filterData(data: RDD[Row]): RDD[Row] = {
    data.filter(row => row.length > 9 && row(9) != null && !row(9).toString.matches("[a-zA-Z]*"))
  }


  def loadData(data: RDD[Row]): RDD[Population] = {
    data.map[Population](row => new Population(row(0).toString, row(1).toString.toInt, row(4).toString, row(9).toString.toDouble))
  }


  def countPopulationForAllYears(data: RDD[Population]): RDD[(String, Iterable[(Int, Double)])] = {
    data
      // посчитаем население для каждой страны и года
      .map { p => ((p.country, p.year), p.populate) }.reduceByKey((a, b) => a + b)
      // перегруппируем данные
      .map { case ((country, year), populate) => (country, (year, populate)) }.groupByKey()
  }


  def countPopulationForLastYear(data: RDD[(String, Iterable[(Int, Double)])]): RDD[(String, Double)] = {
    data
      // найдем максимальные значения для каждой страны
      .map { x => (x._1, x._2.max) }
      //  уберем данные о годе
      .map { case (country, (year, population)) => (country, population) }
  }


  def countPopulationCity(data: RDD[Population]): RDD[(String, Int)] = {
    data
      // посмоторим население для каждого города по каждому году
      .map(x => ((x.country, x.city), (x.year, x.populate)))
      //  удалим города старой переписи
      .reduceByKey((x, y) => if (x._1 >= y._1) x else y)
      .map { case ((country, city), (year, population)) => (country, (city, year, population)) }
      // отфильтруем города у которых население меньше 1000000
      .filter(x => x._2._3 >= 1000000).groupByKey()
      // посчитаем количество городов -  миллионников для каждой страны
      .map(x => (x._1.toString, x._2.size))
  }


  def countCityMillionInEveryCountry(data: RDD[Row], countryMil: RDD[(String, Int)]): RDD[(String, Int)] = {
    val allCountry = data.map(x => (x(0).toString, 0))
    allCountry.union(countryMil).reduceByKey((x, y) => if (x >= y) x else y)
  }


  def loadFile(path: String, sparkSession: SparkSession): RDD[Row] = {
    sparkSession.read.format("csv").option("header", "true").load(path).rdd
  }

}

class Population(var country: String, var year: Int, var city: String, var populate: Double)
