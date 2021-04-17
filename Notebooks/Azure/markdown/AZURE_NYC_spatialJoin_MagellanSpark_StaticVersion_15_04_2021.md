

```scala
%%configure -f
{
    "conf": {
        "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0,harsha2010:magellan:1.0.5-s_2.11,com.esri.geometry:esri-geometry-api:1.2.1,commons-io:commons-io:2.6,org.apache.spark:spark-streaming_2.11:2.2.0,org.apache.spark:spark-sql_2.11:2.2.0",
        "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.11",
        "spark.dynamicAllocation.enabled": false
    }
}
```


```scala
/**
 * @Description: a spatial join based on Filter-refine approach for NYC taxicab data
 * @author: Isam Al Jawarneh
 * @date: 02/02/2019
 *last update: 14/04/2021
 */
```


```scala
sc.version
```


```scala
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
import org.apache.spark.sql.DataFrame
```


```scala
val schemaNYCshort = StructType(Array(
    StructField("vendorId", StringType, false),
    StructField("Pickup_longitude", DoubleType, false),
    StructField("Pickup_latitude", DoubleType, false),
    StructField("Trip_distance", DoubleType, false)))
```


```scala
// a user defined function to get geohash from long/lat point 
val geohashUDF = udf{(curve: Seq[ZOrderCurve]) => curve.map(_.toBase32())}
```


```scala
//"wasb[s]://<BlobStorageContainerName>@<StorageAccountName>.blob.core.windows.net/<path>"
val tripsWithGeoNeigh =spark.read.format("csv").option("header", "true").schema(schemaNYCshort).csv("wasbs://sspark-2021-04-17t10-30-16-344z@ssparkhdistorage.blob.core.windows.net/datasets/nyc.csv")//.withColumn("point", point($"Pickup_longitude",$"Pickup_latitude"))

```


```scala
tripsWithGeoNeigh.show(2)
```


```scala
val precision = 30
```


```scala
//getting plain data from CSV file and use UDF to get geohashes
val trips =spark.read.format("csv").option("header", "true").schema(schemaNYCshort).csv("wasbs://sspark-2021-04-17t10-30-16-344z@ssparkhdistorage.blob.core.windows.net/datasets/nyc.csv").withColumn("point", point($"Pickup_longitude",$"Pickup_latitude"))
val ridesGeohashed = trips.withColumn("index", $"point" index  precision).withColumn("geohashArray1", geohashUDF($"index.curve"))//.select($"id", $"vendorId", $"point",$"geohashArray",$"Trip_distance")
val explodedRidesGeohashed = ridesGeohashed.explode("geohashArray1", "geohash") { a: mutable.WrappedArray[String] => a }
```


```scala
explodedRidesGeohashed.show(2,false)
```


```scala
explodedRidesGeohashed.count()
```


```scala
//explode geohashes covering each neighborhood
val rawNeighborhoodsNYC = spark.sqlContext.read.format("magellan").option("type", "geojson").load("wasbs://sspark-2021-04-17t10-30-16-344z@ssparkhdistorage.blob.core.windows.net/neighborhoods/").select($"polygon", $"metadata"("neighborhood").as("neighborhood")).cache()

val neighborhoodsNYC = rawNeighborhoodsNYC.withColumn("index", $"polygon" index  30).select($"polygon", $"index", 
      $"neighborhood").cache()

val zorderIndexedNeighborhoodsNYC = neighborhoodsNYC.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation","neighborhood")
val geohashedNeighborhoodsNYC= neighborhoodsNYC.withColumn("geohashArray", geohashUDF($"index.curve"))

val explodedgeohashedNeighborhoodsNYC = geohashedNeighborhoodsNYC.explode("geohashArray", "geohash") { a: mutable.WrappedArray[String] => a }
```


```scala
explodedgeohashedNeighborhoodsNYC.count()
```


```scala
//joining geohashed trips with exploded geohashed neighborhood using filter-and-refine approach (.where($"point" within $"polygon") is refine --> using the brute force method ray casting for edge cases or false positives)
val rawTripsJoinedNYC = explodedRidesGeohashed.join(explodedgeohashedNeighborhoodsNYC, explodedRidesGeohashed("geohash") === explodedgeohashedNeighborhoodsNYC("geohash"))/*.select("point", "neighborhood","id")*/.where($"point" within $"polygon")
```


```scala
rawTripsJoinedNYC.count()
```


```scala
val Top_N = rawTripsJoinedNYC.groupBy('neighborhood).count().orderBy($"count".desc)

```


```scala
Top_N.show(10)
```


```scala
//groupBy aggregation by neighborhood (or geohash at a granular level to check everything is OK!)
rawTripsJoinedNYC.groupBy('neighborhood).count().orderBy($"count".desc).count()
```


```scala

```
