/*
import org.apache.log4j._
import org.apache.spark._

import scala.util.Try

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object CropsSize {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    def DataCleanser(data: String) = {
      val crop: String = data.split(",")(4).toString
      val year: String = data.split(",")(2).toString
      val area: Float = Try(data.split(",")(5).toFloat).getOrElse(0.0f)
      val size: Float= Try(data.split(",")(6).toFloat).getOrElse(0.0f)
      (crop,(year,area,size))
    }
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("../SparkScala/apy.csv")

    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val crop = lines.map(DataCleanser)
    // Count up how many times each value (rating) occurs
    val results = crop.reduceByKey((x,y) => {if(x._1 == y._1) (x._1,x._2+y._2) else (x._1,y._1)})
    // Sort the resulting map of (rating, count) tuples
    //val sortedResults = results.collect()

    // Print each result on its own line.
    //results.foreach(println)
  }
}
*/
