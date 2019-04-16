import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object TestWork {
  def main(args: Array[String]) {

    //todo add .gitignore

    //todo create object Constants for constants
    //create method getFilePath (exmaple - getFilePath("unsd-citypopulation-year-both.csv")
    val file1 = "C:\\Users\\Администратор\\Downloads\\Telegram Desktop\\unsd-citypopulation-year-both.csv"
    val file2 = "C:\\Users\\Администратор\\Downloads\\Telegram Desktop\\unsd-citypopulation-year-fm.csv"

    //todo try to use SparkSession intead SparkConf and SparkContext
    val conf: SparkConf = new SparkConf().setAppName("SimpleApp").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    //todo val input1 = spark.read.format....
    //todo to except the header use .option("header","true")
    val input1 = sc.textFile(file1)
    val header1 = input1.first()
    val filterData1 = input1.filter(x => x != header1)
    val data1 = filterData(filterData1)

    val input2 = sc.textFile(file2)
    val header2 = input2.first()
    val filterData2 = input2.filter(x => x != header1)
    val data2 = filterData(filterData2)


    // подсчет населения стран

    // данные из 1 файла
    val countryYear1 = countPopulationForAllYears(data1)
    val populationLastYear1 = countPopulationForLastYear(countryYear1)

    // данные из 2 файла
    val countryYear2 = countPopulationForAllYears(data2)
    val populationLastYear2 = countPopulationForLastYear(countryYear2)

    // объединение результатов из двух файлов и удаление дубликатов
    val unionPopilation = populationLastYear1.union(populationLastYear2).distinct()

    val outputFile = "result.txt"
    unionPopilation.saveAsTextFile(outputFile)
  }

  //todo add method loadData which split input strings and convert RDD[Array[String]] to RDD[someCaseClass]
  //example :
  //case class A(someField: String)
  //val rdd = spark.sparkContext.parallelize(Array("a", "b", "c"))
  //val rddA = rdd.map[A](str => A(str))
  def filterData(data: RDD[String]): RDD[Array[String]] ={
    data.map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)").map(elem => elem.trim))
      .filter(str => str.length > 9 && !str.contains("") && str(1).matches("[^\"]*[a-zA-Z]*") && !str(1).matches("[a-zA-Z]*"))
  }


  def countPopulationForAllYears(data: RDD[Array[String]]): RDD[(String, Iterable[(String, Double)])] ={
    data
      //todo replace 'l._1._1' and etc. to case class fields

      // посчитаем население для каждой страны и года
      .map { x => ((x(0), x(1)), x(9).toDouble)}.reduceByKey((a,b) => a+b)
      // перегруппируем данные
      .map{l => (l._1._1,(l._1._2, l._2))}.groupByKey()
  }


  def countPopulationForLastYear(data: RDD[(String, Iterable[(String, Double)])]): RDD[(String, Double)]  ={
    data
      // найдем максимальные значения для каждой страны
      .map(x => (x._1, x._2.max))
      //  уберем данные о годе
      .map(x => (x._1, x._2._2))
  }
}
