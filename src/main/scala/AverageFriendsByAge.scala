import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SparkSession

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object AverageFriendsByAge {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    def DataCleanser(data: String) = {
      val Name = data.split(",")(1)
      val NoOfFriends = data.split(",")(3).toInt
      (Name, NoOfFriends)
    }

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")


    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("../SparkScala/fakefriends.csv")

    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val ratings = lines.map(DataCleanser)

    // Count up how many times each value (rating) occurs
    val results = ratings.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    // Sort the resulting map of (rating, count) tuples
    val average = results.mapValues(x => x._1 / x._2)
    // Print each result on its own line.
    average.collect().sorted.foreach(println)
  }
}