/*
* Author: Isam M. Al Jawarneh
* state: Experimental
* MeanVariance - streaming - SSS (Spatial Stratified Sampling) Threaded- MemoryStream
* Date Modified : 25 March 2019
*/
package org.it.unibo.performance

import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQuery
import scala.collection.mutable.ListBuffer


class Listener(var numRecs:Long = 0L,var popTotal_from_sampling:Double,var tau_hat_str: Double,var estimated_varianceS_estimated_total:Double,Container:ListBuffer[Long],SAMPLING_FRACTION:Double,MIN_OFFSET:Double,MAX_OFFSET:Double,
sq:StreamingQuery) extends StreamingQueryListener {
  import org.apache.spark.sql.streaming.StreamingQueryListener._

  @volatile private var startTime: Long = 0L
  @volatile private var endTime: Long = 0L

    override def onQueryStarted(event: QueryStartedEvent): Unit = {
     

    }
    override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
      	


	//println("--------------statistics for batch id " + batchno + " are as follows") 
		val popTotal_original: Double = popTotal_from_sampling/SAMPLING_FRACTION
//eq 3.2 pg 78: y_bar_str = tau_hat_str/N (estimated mean from stratified sampling)
	val YbarStr: Double = tau_hat_str/popTotal_from_sampling

// eq 3.5 page 79: V_hat_y_hat_str = V_hat_tau_hat_str/N_square
	val estimated_varianceS_estimated_Mean:Double = estimated_varianceS_estimated_total/(popTotal_original*popTotal_original)
	val SE_SSS:Double = scala.math.sqrt(estimated_varianceS_estimated_Mean)

	println("estimated sum [NhYbarh] is " + tau_hat_str)
	println("estimated mean is " + YbarStr)
	println("population total [Nh] from sampling is " + popTotal_from_sampling)
	println("estimated_varianceS_estimated_total " + estimated_varianceS_estimated_total)
	println("estimated_varianceS_estimated_Mean " + estimated_varianceS_estimated_Mean)
	println("popTotal_original is " + popTotal_original)
	println("SE_SSS is " + SE_SSS)
	//println("queryExecutionTime is " + queryExecutionTime)
	println("streaming query las progress is " + sq.lastProgress)
////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////
    }
    override def onQueryProgress(event: QueryProgressEvent): Unit = {

		///println("query is making progress!! " )
		//val currentProgress = event.progress
		//progressBuffer.append(currentProgress)
		      
//numInputRows The aggregate  number of records processed in each trigger.
		numRecs += event.progress.numInputRows //number of records processed so far
				
//last hundred input rows (last batch interval)
		if(numRecs>=MIN_OFFSET && numRecs<=MAX_OFFSET)
		{popTotal_from_sampling = 0
		estimated_varianceS_estimated_total = 0
		tau_hat_str = 0}

		if(numRecs>=MAX_OFFSET)
		sq.stop()

		println("numRecs is "+ numRecs)
				
    }// end method onQueryProgress

def getNumRecs():Long = {
numRecs
}
  }//end StreamingQueryListener class
