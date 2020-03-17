
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
  .custom spatial-aware stratified on-the-fly sampling method.
  ............................................................*/

  /**
   * Returns a stratified sample without replacement (bernouli) based on the fraction given on each stratum.
   * @param col column that defines strata
   * @param fractions sampling fraction for each stratum. If a stratum is not specified, we treat
   *                  its fraction as zero.
   * @param seed random seed
   * @tparam T stratum type
   * @return a new `DataFrame` that represents the stratified sample
   *
   * @since 1.0.0
   */

def spatialSampleBy(neigh_geohashed_df:DataFrame, points_geohashed_df:DataFrame, samplingRatio: Double): DataFrame = {
	val geoSeq: Seq[String] = neigh_geohashed_df.select("geohash").distinct.rdd.map(r => r(0).asInstanceOf[String]).collect()
	val map = Map(geoSeq map { a => a -> samplingRatio }: _*)
	val samplepointDF = points_geohashed_df.stat.sampleBy("geohash",map,7L)
	return samplepointDF}

 def spatialSRS (df:DataFrame,  fractions: Double, seed: Long ): DataFrame = {
    //require(fractions.values.forall(p => p >= 0.0 && p <= 1.0),
      //s"Fractions must be in [0, 1], but got $fractions.")
    import org.apache.spark.sql.functions.{rand, udf}
    val r = rand(seed)
    val f = udf { ( x: Double) =>
      x < fractions
    }
    df.filter(f(r))
}

}
