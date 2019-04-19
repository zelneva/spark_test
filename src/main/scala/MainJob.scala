import org.apache.spark.sql.SparkSession

object MainJob {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("MainJob")
      .config("spark.master", "local")
      .getOrCreate()


    println("RDD or DF?")
    val choice = scala.io.StdIn.readLine()
    if (choice == "RDD" | choice == "rdd" | choice == "1") {
      RddJob(sparkSession)
    } else if (choice == "DF" | choice == "df" | choice == "2") {
      DfJob(sparkSession)
    }
  }

}
