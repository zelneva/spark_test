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

    val input2 = loadFile(Files.FILE_2, sparkSession)
    val data2 = filterData(input2)
    val population2 = loadData(data2)


    // подсчет населения стран

    val countryYear = countPopulationForAllYears(population1)
    val populationLastYear = countPopulationForLastYear(countryYear)

    val outputFile = "output/populationCountry"
    populationLastYear.saveAsTextFile(outputFile)


    //подсчет городов миллионников

    val countryWithCityMillion = countPopulationCity(population1)
    val allCountryCountCityMillion = countCityMillionInEveryCountry(data1, countryWithCityMillion)

    val outputCityMillion = "output/citiesMillion"
    allCountryCountCityMillion.saveAsTextFile(outputCityMillion)


    // 5 самых крупных городов

    val topCities = top5cities(population1)

    val outputTop5Cities = "output/top5cities"
    topCities.saveAsTextFile(outputTop5Cities)


    // соотношение мужского и женского населения

    val ratioPopulation = calculateRatioPopulation(population2)
    val outputRatioPopulation = "output/ratioPopulation"
    ratioPopulation.saveAsTextFile(outputRatioPopulation)


  }


  def filterData(data: RDD[Row]): RDD[Row] = {
    data.filter(row => row.length > 9 && row(9) != null && !row(9).toString.matches("[a-zA-Z]*"))
  }


  def loadData(data: RDD[Row]): RDD[Population] = {
    data.map[Population](row => new Population(row(0).toString, row(1).toString.toInt, row(4).toString,
      row(9).toString.toDouble, row(3).toString))
  }


  def countPopulationForAllYears(data: RDD[Population]): RDD[(String, Iterable[(Int, Double)])] = {
    data
      // посчитаем население для каждой страны и года
      .map { p => ((p.country, p.year), p.population) }.reduceByKey((a, b) => a + b)
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
      .map(x => ((x.country, x.city), (x.year, x.population)))
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


  def top5cities(data: RDD[Population]): RDD[(String, Iterable[String])] = {
    data
      // посмоторим население для каждого города по каждому году
      .map(x => ((x.country, x.city), (x.year, x.population)))
      //  удалим города старой переписи
      .reduceByKey((x, y) => if (x._1 >= y._1) x else y)
      .map { case ((country, city), (year, population)) => ((country, city), population) }
      // сортируем население по убыванию
      .sortBy(_._2, false)
      .map { case ((country, city), population) => (country, city) }.groupByKey()
      // берем первые 5 городов
      .map { case (country, city) => (country, city.take(5)) }
  }


  def calculateRatioPopulation(data: RDD[Population]): RDD[(String, (String, Double), (String, Double))] = {
    val a = data
    data
      // посмоторим население полов для каждой страны по каждому году
      .map(x => ((x.country, x.sex, x.year), x.population)).reduceByKey((a, b) => a + b)
      .map { case ((country, sex, year), population) => ((country, sex), (year, population)) }
      //удалим данные старой переписи
      .reduceByKey((x, y) => if (x._1 >= y._1) x else y)
      .map { case ((country, sex), (year, population)) => (country, (sex, population, "sex2", 0.0, population)) }
      .reduceByKey((x, y) => (x._1, x._2, y._1, y._2, x._5 + y._5))
      .map { case (country, (sex1, population1, sex2, population2, allPopulation)) =>
        (country, (sex1, population1 / (population1 + population2) * 100), (sex2, population2 / (population1 + population2) * 100))
      }
  }

}


class Population(var country: String, var year: Int, var city: String, var population: Double, var sex: String)
