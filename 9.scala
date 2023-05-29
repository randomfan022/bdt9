import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
object wordcount{
def main(args:Array[String]){
val pathToFile="log.txt"
val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
val sc = new SparkContext(conf)
val wordRDD = sc.textFile(pathToFile).flatMap(_.split(" "))
val wordCountInitRdd = wordRDD.map(word=>(word,1))
val wordCountRdd = wordCountInitRdd.reduceByKey((v1,v2)=>v1+v2)
val highfreqwords = wordCountRdd.filter(x=>x._2>0)
highfreqwords.saveAsTextFile("wordcountsDir")
}
}
