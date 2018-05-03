package jobs

import core.Loggable
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}


object CitiBikeNYCJob {
  
  import util.Distance._
  import core.Loggable._
  
  
  def main(args: Array[String]) {
    
    val conf = new SparkConf()
        .setMaster("local[4]")
        .setAppName("CitiBikeNYCExperiments")
        .set("spark.executor.memory", "1g")
        .set("spark.driver.memory", "1g")
        
    Loggable.setStreamingLogLevels()
    
    val sc = new SparkContext(conf);
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    val bike = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("data/* - Citi Bike trip data.csv")
    
    val bikeDF = bike.toDF("trip_duration", "start_time", "stop_time", "start_station_id", "start_station_name", "start_station_latitude", "start_station_longitude", "end_station_id", "end_station_name", "end_station_latitude", "end_station_longitude", "bike_id", "user_type", "birth_year", "gender")
    
    bikeDF.registerTempTable("bike")
    
    //1
	println("Route Citi Bikers ride the most:")
    sqlContext.sql("SELECT start_station_id,start_station_name,end_station_id,end_station_name, COUNT(1) AS cnt FROM bike GROUP BY start_station_id,start_station_name,end_station_id,end_station_name ORDER BY cnt DESC LIMIT 1").take(1).foreach(println)
    
    //2
	println("Biggest trip and its duration:")
    sqlContext.udf.register("calcDist", calcDist _)
    sqlContext.sql("SELECT start_station_id,start_station_name,end_station_id,end_station_name, trip_duration, calcDist(start_station_latitude,start_station_longitude,end_station_latitude,end_station_longitude) AS dist FROM bike ORDER BY trip_duration DESC, dist DESC LIMIT 1").take(1).foreach(println)
    
    //3
	println("Top 10 Hours of a day they Ride mostly:")
    sqlContext.sql("SELECT date_format(start_time,'H:m'), COUNT(1) as cnt FROM bike GROUP BY date_format(start_time,'H:m') ORDER BY cnt DESC LIMIT 10").take(10).foreach(println)
    
    //4
	println("Most fartest ride:")
    sqlContext.sql("SELECT start_station_id,start_station_name,end_station_id,end_station_name, trip_duration, calcDist(start_station_latitude,start_station_longitude,end_station_latitude,end_station_longitude) AS dist FROM bike ORDER BY dist DESC LIMIT 1").take(1).foreach(println)
    
    //5
	println("Top 10 most popular stations:")
    var popularStationsDF = sqlContext.sql("SELECT start_station_id as station_id,start_station_name as station_name, COUNT(1) as cnt FROM bike GROUP BY start_station_id,start_station_name UNION ALL SELECT end_station_id as station_id,end_station_name as station_name, COUNT(1) as cnt FROM bike GROUP BY end_station_id,end_station_name")
    popularStationsDF.registerTempTable("popularStations")
    sqlContext.sql("SELECT station_id as station_id,station_name, SUM(cnt) as total FROM popularStations GROUP BY station_id,station_name ORDER BY total DESC LIMIT 10").take(10).foreach(println)
    
    //6
	println("Day of the week most rides taken on:")
    sqlContext.sql("SELECT date_format(start_time,'E') AS Day,COUNT(1) AS cnt FROM bike GROUP BY date_format(start_time,'E') ORDER BY cnt DESC LIMIT 1").take(1).foreach(println)
    
    //MLlib
    val bikeRDD = bikeDF.rdd
    
    val features = bikeRDD.map { bike =>
    val gender = if (bike.getInt(14) == 1) 0.0 else if (bike.getInt(14) == 2) 1.0 else  2.0
    val trip_duration = bike.getInt(0).toDouble
    val start_time = bike.getTimestamp(1).getTime.toDouble
    val stop_time = bike.getTimestamp(2).getTime.toDouble
    val start_station_id = bike.getInt(3).toDouble
    val start_station_latitude = bike.getDouble(5)
    val start_station_longitude = bike.getDouble(6)
    val end_station_id = bike.getInt(7).toDouble
    val end_station_latitude = bike.getDouble(9)
    val end_station_longitude = bike.getDouble(10)
    val user_type = if (bike.getString(12) == "Customer") 1.0 else 2.0
    Array(gender, trip_duration, start_time, stop_time, start_station_id, start_station_latitude, start_station_longitude, end_station_id, end_station_latitude, end_station_longitude, user_type)
    }
    
    val labeled = features.map { x => LabeledPoint(x(0), Vectors.dense(x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10)))}
    
    // Split data into training (60%) and test (40%).
    val training = labeled.filter(_.label != 2).randomSplit(Array(0.40, 0.60))(1)
    val test = labeled.filter(_.label != 2).randomSplit(Array(0.60, 0.40))(1)
    
    val test_count = test.count
    
    training.cache
    training.count
    
    
    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(training)
    
    // Compute raw scores on the test set.
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }
	
	println("Predictions:")
	predictionAndLabels.take(10).foreach(println)
    
    val wrong = predictionAndLabels.filter {
    case (label, prediction) => label != prediction
    }
    
    val wrong_count = wrong.count
    
    val accuracy = 1 - (wrong_count.toDouble / test_count)
    println(s"Accuracy model1: " + accuracy)
  }
}
