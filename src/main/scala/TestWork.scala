import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object TestWork {
  def main(args: Array[String]) {

    val file1 = "C:\\Users\\Администратор\\Downloads\\Telegram Desktop\\unsd-citypopulation-year-both.csv"
    val file2 = "C:\\Users\\Администратор\\Downloads\\Telegram Desktop\\unsd-citypopulation-year-fm.csv"

    val conf: SparkConf = new SparkConf().setAppName("SimpleApp").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val input1 = sc.textFile(file1)
    val header1 = input1.first()
    val filterData1 = input1.filter(x => x != header1)
    val data1 = filterData1.map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)").map(elem => elem.trim))
      .filter(str => str.length > 9 && !str.contains("") && str(1).matches("[^\"]*[a-zA-Z]*") && !str(1).matches("[a-zA-Z]*"))


    val input2 = sc.textFile(file2)
    val header2 = input2.first()
    val filterData2 = input2.filter(x => x != header1)
    val data2 = filterData2.map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)").map(elem => elem.trim))
      .filter(str => str.length > 9 && !str.contains("") && str(1).matches("[^\"]*[a-zA-Z]*") && !str(1).matches("[a-zA-Z]*"))


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


  def countPopulationForAllYears(data: RDD[Array[String]]): RDD[(String, Iterable[(String, Double)])] ={
    data
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
