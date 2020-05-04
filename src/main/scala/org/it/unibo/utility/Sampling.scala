
/*
* Author: Isam M. Al Jawarneh
* state: Experimental
* MeanVariance - streaming - SSS (Spatial Stratified Sampling) Threaded- MemoryStream
* Date Modified : 25 March 2019
*/

package org.it.unibo.utility

import org.apache.spark.sql.DataFrame

class Sampling {

/*............................................................
  custom spatial-aware stratified micro-batching-based sampling method.
  ............................................................*/

 

def spatialSampleBy(neigh_geohashed_df:DataFrame, points_geohashed_df:DataFrame, samplingRatio: Double): DataFrame = {
	val geoSeq: Seq[String] = neigh_geohashed_df.select("geohash").distinct.rdd.map(r => r(0).asInstanceOf[String]).collect()
	val map = Map(geoSeq map { a => a -> samplingRatio }: _*)
		
	 val tossAcoin = rand(7L)
    val getSamplingRate = udf { (geohash: Any, rnd: Double) =>
      rnd < map.getOrElse(geohash.asInstanceOf[String], 0.0)
    }
val samplepointDF =  points_geohashed_df.filter(getSamplingRate1(map, 0.0)($"geohash", tossAcoin))
	return samplepointDF}

 def getSamplingRate1(map: Map[String, Double], defaultValue: Double) = udf{
  (geohash: String, rnd: Double) =>
      rnd < map.getOrElse(geohash.asInstanceOf[String], 0.0)
}

}
