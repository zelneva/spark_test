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

    val filterText = textData.map(line => line.split(" ")).filter(x => x.length > 1)
    filterText.map(x => x.length).foreach(println)
    filterText.map( x => (x(0), x(1))).foreach(println)
    val data = filterText.map( x => (x(0), x(1).toInt)).reduceByKey((x,y) => x+y).foreach(println)



    //    val data = sc.parallelize(Array(1, 2, 3, 4, 1, 2))
    //    val dataCube = data.map(x=>x*x*x).collect().foreach(println)

    //    val uniqueNumber = numberData.distinct.foreach(println)


    //    val numberWithoutSpace = numberData.flatMap(line => line.split(" "))

    //    val sum = println(data.reduce((x, y) => x + y))

    //    val countArray = data.countByValue().foreach(println)

    //    val topElement = data.top(5).foreach(println)

    //    val linesRDD = textData.filter(line => line.contains("f"))
    //    linesRDD.take(5).foreach(println)


    //    val pairs = textData.flatMap(line => line.split(" ")).flatMap(l => l.split(""))map (x => (x, 1)) reduceByKey ((x, y) => x + y)
    //    pairs.foreach(println)

    //    val dataSeq = Seq(("а", 3), ("b", 4), ("а", 1))
    //    val seqData = sc.parallelize(dataSeq)
    //    val z = seqData.groupByKey.foreach(println)


  }

  class SimpleCSVHeader(header:Array[String]) extends Serializable {
    val index = header.zipWithIndex.toMap
    def apply(array:Array[String], key:String):String = array(index(key))
  }


}
