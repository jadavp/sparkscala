import org.apache.spark.sql.SparkSession

object testspark extends App{

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  spark.read.format("csv").load("/mnt/els/automount/mendeley-canonical-documents/")

}
