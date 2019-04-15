import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setAppName("SimpleApp").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val text = "text.txt"
    val numbers = "int.txt"
    val textData = sc.textFile(text)
    val numberData = sc.textFile(numbers)

    val filterText = textData.map(line => line.split(" ")).filter(x => x.length > 1).map(x => (x(0), x(1).toInt)).groupByKey.mapValues(x => x.max)
    val data = numberData.map(line => line.split(" ")).filter(x => x.length > 1).map(x => (x(0), x(1).toInt)).groupByKey.mapValues(x => x.max)

    data.union(filterText).distinct().foreach(println)

  }
}
