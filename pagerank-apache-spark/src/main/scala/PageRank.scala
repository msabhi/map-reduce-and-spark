/**
  * Created by Abhi on 10/31/16.
  */

import java.io.{FileWriter, BufferedWriter}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import parser.Bz2WikiParser

object PageRank {

  //Parse the line to get a pagename-adjacencylist string
  def pageFileParser(line:String): Array[String] ={
    val delimLoc = line.indexOf(':')
    val pageName = line.substring(0, delimLoc)
    val adjSet = Bz2WikiParser.parser(line)

    adjSet match {
      case null => null // if the set is null, there was some issue in parsing
      case _ => {
        val adjSetString = adjSet.toString.substring(1, adjSet.toString.length-1)
        (pageName + "~~~" + adjSetString).split("~~~") // return a pagename-adjacencylist string
      }
    }
  }

  def evaluate[T](rdd:RDD[T]) = {
    rdd.sparkContext.runJob(rdd,(iter: Iterator[T]) => {
      while(iter.hasNext) iter.next()
    })
  }



  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Calculate page rank")
    //val conf = new SparkConf().setMaster("local").setAppName("Calculate page rank")
    val sparkContext = new SparkContext(conf)



    val iters = if (args.length > 2) args(2).toInt else 10
    val inputFile = args(0)
    val outputFile = args(1)
    val graph = sparkContext.textFile(inputFile).map(pageFileParser).filter(a=>a!=null).
      map(fields=>{if(fields.length==1)(fields(0).trim, new Array[String](0)) else (fields(0).trim, fields(1).split(","))}).persist()
    val noOfNodes = graph.count().toDouble
    var ranks = graph.mapValues(v=>1.0/noOfNodes)

    for (i <- 1 to iters) {
      val deltaLoss = sparkContext.accumulator(0.0)
      var contribs = graph.join(ranks).flatMap { case (pgid, (urls, rank)) =>
        val size = urls.size
        size match {
          case 0 => {
            deltaLoss += (rank)
            None
          }
          case _ => {
            urls.map(url => (url.trim, rank / size))
          }
        }
      }

      //val contribcount = contribs.count()
      evaluate(contribs)
      val newccontribs = contribs.subtractByKey(graph).mapValues(x=> {deltaLoss.add(x)}).count()
      val acc = deltaLoss.value
      val newcontribs = contribs.reduceByKey(_ + _).mapValues(v=>0.15/noOfNodes + 0.85 * v + 0.85 * acc/noOfNodes)
      val noInlinksRanks = ranks.subtractByKey(newcontribs).mapValues(v => 0.15/noOfNodes + 0.85 * acc/noOfNodes)
      ranks = newcontribs.union(noInlinksRanks)
    }
    sparkContext.parallelize(ranks.sortBy(a=> -1 * a._2).take(100),1).saveAsTextFile(outputFile)
    sparkContext.stop()
  }
}
