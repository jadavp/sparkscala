import java.nio.charset.CodingErrorAction

import org.apache.log4j._
import org.apache.spark._

import scala.io.{Codec, Source}

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object PopularMovie {

  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()

    val lines = Source.fromFile("../SparkScala/u.item").getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    movieNames
  }


  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        def f(data:String): String ={
          data.split("\t")(1)
        }
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")

    var moviename = sc.broadcast(loadMovieNames)
   
    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("../SparkScala/u.data")
    
    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val movies = lines.map(x => (x.split("\t")(1).toInt,1))
    
    // Count up how many times each value (rating) occurs
    val results = movies.reduceByKey((x,y) => x+y).map(x => (x._2,x._1)).sortByKey().collect()

    val resultswithnames = results.map(x => (moviename.value(x._2),x._1))
    
    // Print each result on its own line.
    resultswithnames.foreach(println)
  }
}
