/*
* Author: Isam M. Al Jawarneh
* state: Experimental
* MeanVariance - streaming - SSS (Spatial Stratified Sampling) Threaded- MemoryStream
* Date Modified : 25 March 2019
*/
 package org.it.unibo.utility

import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType,
IntegerType
}


import org.apache.spark.sql.DataFrame

import magellan._
import magellan.index.ZOrderCurve
import magellan.{Point, Polygon}
import org.apache.spark.sql.magellan.dsl.expressions._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.ForeachWriter

import scala.collection.mutable

class Helper{


 /**
   * Returns a list of geohashes covering each polygon (neigborhood)
   * @param spark SparkSession
   * @param geohashPrecision the precision of geohash
   * @param filePath the path of the file containing polygons (neigborhood), it is currently geoJson
   * @since 1.0.0
   */

def geohashedNeighborhoods(spark: SparkSession, geohashPrecision: Int, filePath: String): DataFrame = 

{

import spark.implicits._
// preparing the neighborhoods table (static table) .... getting geohashes covering every neighborhood and exploding it
//so that each neighborhood has many geohashes

// /home/ec2-user/spark/datasets/neighborhoods/


// this will be executed only one time - batch mode 
val rawNeighborhoods = spark.sqlContext.read.format("magellan").option("type", "geojson").load(filePath).select($"polygon", $"metadata"("neighborhood").as("neighborhood")).cache()

val neighborhoods = rawNeighborhoods.withColumn("index", $"polygon" index geohashPrecision).select($"polygon", $"index", 
      $"neighborhood").cache()

val zorderIndexedNeighborhoods = neighborhoods.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation","neighborhood")
val geohashedNeighborhoods= neighborhoods.withColumn("geohashArray", geohashUDF($"index.curve"))

val explodedgeohashedNeighborhoods = geohashedNeighborhoods.explode("geohashArray", "geohash") { a: mutable.WrappedArray[String] => a }

//explodedgeohashedNeighborhoods.show(10)


explodedgeohashedNeighborhoods

}


def geohashedRawData(spark: SparkSession,inputStream: MemoryStream[(Int,Point,Double)], geohashPrecision: Int): DataFrame = {

import spark.implicits._
val rawTrips = inputStream.toDS().toDF("id","point", "Trip_distance")

//rawTrips.show(10)
val ridesGeohashed = rawTrips.withColumn("index", $"point" index geohashPrecision).withColumn("geohashArray", geohashUDF($"index.curve")).select($"id", $"point",$"geohashArray",$"Trip_distance")

val explodedRidesGeohashed = ridesGeohashed.explode("geohashArray", "geohash") { a: mutable.WrappedArray[String] => a }

explodedRidesGeohashed

}


val geohashUDF = udf{(curve: Seq[ZOrderCurve]) => curve.map(_.toBase32())}

object RawSchema {

//the schema for the NYC points

val schemaNYC = StructType(Array(
StructField("id", IntegerType, false),
    StructField("vendorId", StringType, false),
    StructField("lpep_pickup_datetime", StringType, false),
    StructField("Lpep_dropoff_datetime", StringType, false),
    StructField("Store_and_fwd_flag",StringType, false),
    StructField("RateCodeID", IntegerType, false),
    StructField("Pickup_longitude", DoubleType, false),
    StructField("Pickup_latitude", DoubleType, false),
    StructField("Dropoff_doubleitude", DoubleType, false),
    StructField("Dropoff_latitude", DoubleType, false),
    StructField("Passenger_count", IntegerType, false),
    StructField("Trip_distance", DoubleType, false),
    StructField("Fare_amount", StringType, false),
    StructField("Extra", StringType, false),
    StructField("MTA_tax", StringType, false),
    StructField("Tip_amount", StringType, false),
    StructField("Tolls_amount", StringType, false),
    StructField("Ehail_fee", StringType, false),
    StructField("improvement_surcharge", StringType, false),
    StructField("Total_amount", DoubleType, false),
    StructField("Payment_type", IntegerType, false),
    StructField("Trip_type", IntegerType, false)))

}

}
