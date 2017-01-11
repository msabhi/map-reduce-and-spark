import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object MmulSparse {
    val on_csv = """\s*,\s*""".r

    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("mmul").setMaster("local")
        val sc   = new SparkContext(conf)

        val colsRDD = sc.textFile("_test/dataA.csv").map(line => {val lines = on_csv.split(line);
            (lines(1).toInt,(lines(0).toInt, lines(2).toDouble))})
        println("Size of dataA RDD: " + colsRDD.count)

        val rowsRDD = sc.textFile("_test/dataB.csv").map(line => {val lines = on_csv.split(line);
            (lines(0).toInt,(lines(1).toInt, lines(2).toDouble))})
        println("Size of dataB RDD: " + rowsRDD.count)

        val cdata = colsRDD.join(rowsRDD).map( {case (key, ( (rowInd, rowVal), (colInd, colVal)) ) => ((rowInd, colInd), rowVal*colVal)}).reduceByKey(_+_)

        println("Size of output RDD: " + cdata.count)
        cdata.map{case ((x,y),z)=>x+" ,"+y+" ,"+z}.saveAsTextFile("output")
        
        sc.stop()
    }
}

// vim: set ts=4 sw=4 et:
