import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.xml.XML;


object test extends App {
  val conf = new SparkConf().setMaster("local[*]").setAppName("Checkingdata")
  val sc = new SparkContext(conf)
  val sQLContext = new SQLContext(sc)

  import sQLContext.implicits._

  val data = XML.loadFile("C:\\Users\\611052474\\Downloads\\xmldata\\sample.xml")
  val name = (data \\ "trk" \\ "name").text
  val ele = (data \\ "trk" \\ "trkseg" \\ "ele").toList
  val lat = (data \\ "trk" \\ "trkseg" \\ "@lat").toList
  lat.foreach(println)
  // val broadVar=sc.broadcast(name)
  //ele.foreach(println)
  val finalOutput = ele.map(x => (name, x.text, x.text)).toDF("Name", "Element", "Latitutde")
  finalOutput.show()
  //val cdf= sQLContext.createDataFrame(Seq(name),Seq(ele))
  //val result= Seq((name,ele)).toDF("name","value")
  //println(result.show())
}
