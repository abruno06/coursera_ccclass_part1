import org.apache.spark._
import org.apache.spark.streaming._

val conf = new SparkConf().setAppName("WC").setMaster("master")
val ssc = new StreamingContext(conf, Seconds(1))