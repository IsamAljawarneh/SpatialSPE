

//spark-submit  --master local[*] --class SSS    SSS-assembly-0.1.0-SNAPSHOT.jar 999999 100000 1000 0.2 30 /home/isam/Desktop/spatial/data/neighborhoods/ /home/isam/Desktop/spatial/data/tagged1/ 1000


/*
* 
SpatialSPE
Author: Isam M. Al Jawarneh
* state: Experimental
* MeanVariance - streaming - SSS (Spatial Stratified Sampling) Threaded- MemoryStream
* Last Modified : 23 August 2019
* N.B. in this example we use memory stream for simplicity. in production and to compare performance with other alternatives you need to
* use an ingestion system such as Apache Kafka
*/

import java.util.Date
import util.control.Breaks._
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.util.random.XORShiftRandom
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.ForeachWriter
import magellan._
import magellan.index.ZOrderCurve
import magellan.{Point, Polygon}

import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{collect_list, collect_set}
import org.apache.spark.sql.SQLContext
import org.apache.log4j.{Level, Logger}
import scala.collection.mutable
import scala.concurrent.duration.Duration
import java.io.{BufferedWriter, FileWriter}
import org.apache.commons.io.FileUtils
import java.io.File
import scala.collection.mutable.ListBuffer
import java.time.Instant
import org.apache.spark.util.CollectionAccumulator
import org.it.unibo.utility.Sampling
import org.it.unibo.utility.Helper
import org.it.unibo.performance.Statistics

object SSS  extends App {



	val spark = SparkSession
 	 .builder
 	 .appName("SSS")
 	 .getOrCreate()

	import spark.implicits._

val rootLogger = Logger.getRootLogger()
rootLogger.setLevel(Level.ERROR)

val so = new Sampling
val helper = new Helper
val statistics = new Statistics
val fileStreamDf = spark.read.format("csv").option("header", "true").schema(helper.RawSchema.schemaNYC).csv(args(6)).withColumn("point", point($"Pickup_longitude",$"Pickup_latitude"))


 val inputStream = new MemoryStream[(Int,Point,Double)](1, spark.sqlContext)
val filePath = args(5)

val geohashedRawRides = helper.geohashedRawData(spark, inputStream,Variables.PRECISION)
val geohashedNeigboors = helper.geohashedNeighborhoods(spark,Variables.PRECISION,filePath)
val samplepointDF_SSS = so.spatialSampleBy(geohashedNeigboors,geohashedRawRides,Variables.SAMPLING_FRACTION)


/*............................................................
  .			SSS Statistics			     .
  ............................................................*/
println("..................SSS statistics................... ")

/*semantics:
per-strat-count: Nh
*/

/* 
quantity = [(1-nh/Nh) * N_Square_h * variance_h/nh]: for each stratum (geohash),  required for computing the variance
v-hat(tau_hat_str) = sigma [(1-nh/Nh) * N_Square_h * variance_h/nh] // page 79 theory of stratified sampling
Nh = per-strat-count-total
nh  = per-strat-count , aka resulted from sampling
s(small)_square_h = per-strat-var
Nh = (col("per-strat-count")/lit( Variables.SAMPLING_FRACTION))
*/
val samplingStatisticsDF  = statistics.statisticsSSSPerStrata(spark, samplepointDF_SSS, Variables.SAMPLING_FRACTION)

/*= samplepointDF_SSS.groupBy($"geohash")
  .agg(
    avg($"Trip_distance").as("per-strat-mean"), variance($"Trip_distance").as("per-strat-var")
	,sum($"Trip_distance").as("per-strat-sum"),
	count($"Trip_distance").cast("double").as("per-strat-count")

  ).withColumn("NhYbarh",col("per-strat-mean")* col("per-strat-count"))
	.withColumn(
	"quantity",when($"per-strat-var".isNaN, lit(0)).otherwise(

(lit(1) - (col("per-strat-count")/(col("per-strat-count")/lit( Variables.SAMPLING_FRACTION)))) * (col("per-strat-count")/lit( Variables.SAMPLING_FRACTION)) * (col("per-strat-count")/lit( Variables.SAMPLING_FRACTION)) * (col("per-strat-var")/col("per-strat-count")))
 								)*/


var batchno: Long  = 0L
var foreachWriterTime: Long = 0 //@volatile 
var tau_hat_str: Double = 0 // tau_hat_str, estimated sum (for an item value) from the stratification, eq 3.1 page 78
var popTotal_from_sampling:Double = 0 //Nh
/////////////////////////////////////////////
//calculating Estimated mean on each Trigger/
/////////////////////////////////////////////

var tau_hat_str_trigger: Double = 0 
var popTotal_from_sampling_trigger:Double = 0
var currentMean_Accumulator:scala.collection.mutable.Map[Int,Double] = scala.collection.mutable.Map()
val currentMean_Buff = new scala.collection.mutable.ListBuffer[Double]()
var meanCount:Int = 0
var estimated_varianceS_estimated_total:Double = 0 //eq 3.4 pg 79


// is it possible to put this in a separate class such as this example!
//https://docs.databricks.com/_static/notebooks/structured-streaming-etl-kafka.html
val writer = new ForeachWriter[Row]() {
      // true means that all partitions will be opened
      override def open(partitionId: Long, version: Long): Boolean = true
 
/********************************************************/
/*---------PROCESS Method ------------------------------*/
/********************************************************/

      override def process(row: Row): Unit = {
val currentTime = System.currentTimeMillis()

println("numRecs so far in the process stage is " + numRecs)

/////////////////////////////////////////////
//calculating Estimated mean on each Trigger/
/////////////////////////////////////////////
tau_hat_str_trigger = tau_hat_str_trigger + row.getAs("NhYbarh").asInstanceOf[Double]
popTotal_from_sampling_trigger = popTotal_from_sampling_trigger + row.getAs("per-strat-count").asInstanceOf[Double]


if(numRecs>=( Variables.MAX_OFFSET -  Variables.RECORDS_PER_BATCH_INTERVAL) && numRecs<= Variables.MAX_OFFSET)
	{
//eq 3.1 pg 78--> tau_hat_str = sigma(h=1--> H) Nh*Y_bar_h
tau_hat_str = tau_hat_str + row.getAs("NhYbarh").asInstanceOf[Double]
popTotal_from_sampling = popTotal_from_sampling + row.getAs("per-strat-count").asInstanceOf[Double]
estimated_varianceS_estimated_total = estimated_varianceS_estimated_total + row.getAs("quantity").asInstanceOf[Double]

}//end if
println(row.getAs("geohash").asInstanceOf[String] + " , " + /*popTotal_from_sampling + */row.getAs("per-strat-count").asInstanceOf[Double] + ", " + row.getAs("per-strat-mean").asInstanceOf[Double] + ",--" + row.getAs("per-strat-sum").asInstanceOf[Double] + 
" , " + popTotal_from_sampling + " , " + row.getAs("quantity").asInstanceOf[Double] + ", " + estimated_varianceS_estimated_total + ", " + row.getAs("per-strat-var").asInstanceOf[Double])


      }
 
      override def close(errorOrNull: Throwable): Unit = {
        // do nothing
      }
    }//END class foreachwriter 


//----------------------monitoring & Throughput/latency calculations----------------------//


 val progressBuffer = new scala.collection.mutable.ListBuffer[StreamingQueryProgress]()


 private var numRecs: Long = 0L // @volatile
 private var sumNumRowsUpdatedStateful: Long = 0L //@volatile
 var triggerExecution:Long = 0L //@volatile


class Listener extends StreamingQueryListener {
  import org.apache.spark.sql.streaming.StreamingQueryListener._
    override def onQueryStarted(event: QueryStartedEvent): Unit = {
     

    }



/********************************************************/
/*---------onQueryTerminated ---------------------------*/
/********************************************************/


    override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
     
	println("total number of records is "  + numRecs)

println("--------------statistics for batch id " + batchno + " are as follows") 
	val popTotal_original: Double = popTotal_from_sampling/ Variables.SAMPLING_FRACTION
//eq 3.2 pg 78	
val YbarStr: Double = tau_hat_str/popTotal_from_sampling
//eq 3.5 pg 79 --> V_hat_y_bar_str = V_hat_y_tau_hat_str/N_square
	val estimated_varianceS_estimated_Mean:Double = estimated_varianceS_estimated_total/(popTotal_original*popTotal_original)


	println("##### estimated total [ tau_hat_str] is " + tau_hat_str)
	println("##### estimated mean [YbarStr] is " + YbarStr)
	println("##### population total [Nh] from sampling is " + popTotal_from_sampling)
	println("##### estimated_varianceS_estimated_total " + estimated_varianceS_estimated_total)
	println("##### estimated_varianceS_estimated_Mean " + estimated_varianceS_estimated_Mean)
	println("##### popTotal_original is " + popTotal_original)
	
	println("streaming query las progress is " + sq.lastProgress)



println("SSS means" + args(3).toString + args(4).toString)
for (index <- 0 until currentMean_Buff.length - 1) {

println(currentMean_Buff(index))

}

 for (index <- 0 until monitorProgress.length - 1) {

    val currentProgress = monitorProgress(index)
	println("batchid is " + currentProgress.batchId)

breakable{
	try{

val stateOperatorsNumRowsUpdated = currentProgress.stateOperators(0).numRowsUpdated
val stateOperatorsNumRowsTotal = currentProgress.stateOperators(0).numRowsTotal
	println("numRowsUpdated " + stateOperatorsNumRowsUpdated + "\t" + "numRowsTotal " + stateOperatorsNumRowsTotal + "\t" + currentProgress.timestamp)
	println("recent progress triggerExecution is  " + currentProgress.durationMs.get("triggerExecution").toLong)
val inputRowsPerSecond = currentProgress.sources(0).inputRowsPerSecond
val processedRowsPerSecond	= currentProgress.sources(0).processedRowsPerSecond

	println("inputRowsPerSecond is  " + inputRowsPerSecond)
	println("processedRowsPerSecond is  " + processedRowsPerSecond)
	triggerExecution = triggerExecution + currentProgress.durationMs.get("triggerExecution").toLong
	sumNumRowsUpdatedStateful = sumNumRowsUpdatedStateful + stateOperatorsNumRowsUpdated
}//end try

		catch {
		case e:ArrayIndexOutOfBoundsException => println(e)
		}//end catch
	}//end breakable

}//end for 


    }//end onQueryTerminated


/********************************************************/
/*---------onQueryProgress ------------------------------*/
/********************************************************/

    override def onQueryProgress(event: QueryProgressEvent): Unit = {

		println("query is making progress!! " )
		
 val lastProgress = sq.lastProgress
if(lastProgress.sources(0).inputRowsPerSecond > 0.0)
          {monitorProgress.append(lastProgress)
println("progress " + sq.lastProgress)
}
		      
//numInputRows The aggregate  number of records processed in each trigger.
		numRecs += event.progress.numInputRows //number of records processed so far
		
print("numRecs so far in the OnQueryProgress stage is " + numRecs)

val currentMean:Double = tau_hat_str_trigger / popTotal_from_sampling_trigger


	
//last cycle input rows (last batch interval)
		if(numRecs>=( Variables.MAX_OFFSET -  Variables.RECORDS_PER_BATCH_INTERVAL) && numRecs<= Variables.MAX_OFFSET)
		{popTotal_from_sampling = 0
		estimated_varianceS_estimated_total = 0
		tau_hat_str = 0}

		if(numRecs>= Variables.MAX_OFFSET)
		{sq.stop()}//END if

		println("numRecs is "+ numRecs)
				
    }//END onQueryProgress METHOD
  }
  
  lazy val listener = new Listener

   spark.streams.addListener(listener)


//.............starting the stream...................//


val sq :StreamingQuery = samplingStatisticsDF.writeStream


	.trigger(Trigger.ProcessingTime(args(7).toLong))
	.foreach(writer)
	 .outputMode("complete")
   	 .start()
	 


var minOffset = 0
var maxOffset =  Variables.RECORDS_PER_BATCH_INTERVAL - 1

 val monitorProgress = new scala.collection.mutable.ListBuffer[StreamingQueryProgress]()
  new Thread(new Runnable() {
    override def run(): Unit = {

	
      var currentBatchId = -1L
      while (sq.isActive ) {

val batchRecordsDF = fileStreamDf.select("*").filter(col("id").between(minOffset,maxOffset))

val geoSeq: Seq[(Int,Point,Double)] = batchRecordsDF.select("id","point","Trip_distance").rdd.map(r => (r(0).asInstanceOf[Int],r(1).asInstanceOf[Point], r(2).asInstanceOf[Double])).collect()

//since we are here feeding the memory stream with every clock tick (trigger). then we do not even need to restart the query
//just start sampling directly beforehand once constructing the hundredDF.
println("batch id " + batchno + " has started") 


// ADDING DATA EVERY BATCH INTERVAL
 inputStream.addData(geoSeq)
        Thread.sleep( Variables.ARRIVAL_RATE)

	minOffset = maxOffset + 1
	maxOffset = maxOffset +  Variables.RECORDS_PER_BATCH_INTERVAL
	batchno = batchno + 1

       
      }
    }
  }).start()

 sq.awaitTermination()


object Variables {
val MAX_OFFSET = args(0).toInt
  val RECORDS_PER_BATCH_INTERVAL = args(1).toInt
var ARRIVAL_RATE = args(2).toInt //batch interval
val SAMPLING_FRACTION:Double = args(3).toDouble //for example 0.2

val PRECISION = args(4).toInt //for example 30
}

}
