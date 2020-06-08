
import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.SparkSession

object HTMLScapper extends App{

def linetotuple(data:String):(String,String)={
  try{
    val s1=data.split(" ")(0).trim.substring(1)
    val s2 = data.split("[<>]")(2)
    (s1,s2)}
  catch{
    case e:ArrayIndexOutOfBoundsException => null
  }
}
  val config = new SparkConf()
  config.setMaster("local[*]")
  config.setAppName("HTMLScapper")
  config.set("spark.driver.bindAddress", "127.0.0.1")
  val sc = new SparkContext(config)
  Logger.getLogger("org").setLevel(Level.ERROR)
  val lines = sc.textFile("../SparkScala/New_Dict")
  val dat = lines.map(linetotuple)
  dat.foreach(println)



}