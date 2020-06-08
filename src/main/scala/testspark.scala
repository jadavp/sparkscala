import org.apache.spark.sql.SparkSession
import org.apache.spark


object testspark extends App{

  val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
  import spark.implicits._
  val data = spark.read.format("csv").load("../SparkScala/apy.csv").toDF()
  data.createOrReplaceTempView("people")

  val sqlw = spark.sql("select avg(*) from people where _c4 = 'Rice' ORDER BY _c6 DESC")
  val sql = spark.sql("")
  //data.select($"_c1",$"_c2",$"_c3",$"_c4",$"_c5",$"_c6").show

}
