
/*
* Author: Isam M. Al Jawarneh
* state: Experimental
* MeanVariance - streaming - SSS (Spatial Stratified Sampling) Threaded- MemoryStream
* Date Modified : 25 March 2019
*/

package org.it.unibo.performance

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class Statistics {


def statisticsSSSPerStrata(spark: SparkSession,samplePointsSSS:DataFrame,samplingFraction: Double) : DataFrame = 

{
import spark.implicits._

samplePointsSSS.groupBy($"geohash")
  .agg(
    avg($"Trip_distance").as("per-strat-mean"), variance($"Trip_distance").as("per-strat-var")//.cast("double"),
,sum($"Trip_distance").as("per-strat-sum"),
	count($"Trip_distance").cast("double").as("per-strat-count")
	//,max($"Trip_distance").as("per-strat-max")
	//, skewness($"Trip_distance").as("per-strat-skewness")
  ).withColumn("NhYbarh",col("per-strat-mean")* col("per-strat-count"))
	.withColumn(
	"quantity",when($"per-strat-var".isNaN, lit(0)).otherwise(

(lit(1) - (col("per-strat-count")/(col("per-strat-count")/lit(samplingFraction)))) * (col("per-strat-count")/lit(samplingFraction)) * (col("per-strat-count")/lit(samplingFraction)) * (col("per-strat-var")/col("per-strat-count")))
 								)
	

}



}
