import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.Range.Double

object TestWork {
  def main(args: Array[String]) {

    val file1 = "C:\\Users\\Администратор\\Downloads\\Telegram Desktop\\unsd-citypopulation-year-both.csv"
    val file2 = "C:\\Users\\Администратор\\Downloads\\Telegram Desktop\\unsd-citypopulation-year-fm.csv"

    val conf: SparkConf = new SparkConf().setAppName("SimpleApp").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val input = sc.textFile(file1)
    val header = input.first()
    val filterData = input.filter(x => x != header)
    val data = filterData.map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)").map(elem => elem.trim)).filter(str => str.length > 9 && !str.contains(""))


    //подсчет населения стран
    val populationData = data.filter(x => x(9).matches("[^\"]*[a-zA-Z]*") && !x(9).matches("[a-zA-Z]*") )
      .map(x => (x(0),x(9).toFloat))
    populationData.reduceByKey((x,y) => x+y).foreach(println)




  }
}
