import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object CropsSize  extends App {

  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
  import spark.implicits._


  val data = spark.read.format("csv").load("../SparkScala/apy.csv").toDF()
  data.write.parquet("../SparkScala/appp")

  //data.createOrReplaceTempView("people")
  //val sqlw = spark.sql("select * from people where Crop = 'Rice' ORDER BY Production DESC").take(30)*/

}
