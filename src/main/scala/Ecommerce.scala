import org.apache.log4j._
import org.apache.spark._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Ecommerce {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    def datafilter(data: String) = {
      val customerId = data.split(",")(0).toInt
      val spend = data.split(",")(2).toFloat
      (customerId,spend)
    }

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("../SparkScala/customer-orders.csv")

    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val data = lines.map(datafilter)

    // Count up how many times each value (rating) occurs
    val results = data.reduceByKey((x,y) => x+y)

    // Sort the resulting map of (rating, count) tuples
    val sortedresult = results.map(x => (x._2,x._1)).sortByKey()
    //val sortedResults = results.toSeq.sortBy(_._1)

    val finalresult = sortedresult.collect()
    // Print each result on its own line.
   for(d <- sortedresult)
     {
       val amount = d._1
       val customerId = d._2
       println(s"$amount:$customerId")
     }
  }
}
