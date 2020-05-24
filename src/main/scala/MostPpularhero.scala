import java.nio.charset.CodingErrorAction

import org.apache.log4j._
import org.apache.spark._

import scala.io.{Codec, Source}

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object MostPpularhero {


  def loadHeroNames(line: String) : Option[(Int, String)] = {
    val data = line.split('\"')
    if(data.length > 1)
      {
        return Some(data(0).trim().toInt, data(1))
      }
    else
      {
        None
      }
  }

  def dataextractor(data:String) ={
    val total = data.split(" ")
    (data.split(" ")(0).toInt,total.length-1)
  }
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)



    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")
    val heroname = sc.textFile("../SparkScala/Marvel-names.txt")
    val heroRDD = heroname.flatMap(loadHeroNames)

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("../SparkScala/Marvel-graph.txt")
    
    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val hero = lines.map(dataextractor)
    val herocount = hero.reduceByKey((x,y) => x+y).map(x  => (x._2,x._1)).sortByKey()
    herocount.max()


  }
}
