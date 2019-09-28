package org.crealytics.task

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DecimalType
import org.apache.hadoop.fs._;

case class InputData(Time :Long, Value:Double)

object TimeSeriesTask {
  
  
  def generateResult(inputDF: DataFrame) : DataFrame= {
						   
	      Logger.getLogger("org").setLevel(Level.ERROR)
    
	      
				val windSpec=Window.orderBy(inputDF.col("Time")).rangeBetween(-60, 0)//Specifying Rolling Time window of length 60 seconds
				
				//Using Spark Sql Windows function for working with the range of Input rows
				val result=inputDF.withColumn("N_O",count("Value").over(windSpec)).withColumn("Roll_Sum",sum("Value").over(windSpec) cast DecimalType(7,5))
				             .withColumn("Min_Value",min("Value").over(windSpec)).withColumn("Max_Value",max("Value").over(windSpec))
                                                                      
				
			//	result.show(100)
				
				result
					   }
  
 
  def main(args: Array[String]){
    
    //Spark Session Creation
   val spark = SparkSession.builder
               .appName("TimeSeries")  
               .master("local[*]") 
               .getOrCreate()  
   
   Logger.getLogger("org").setLevel(Level.ERROR)
   
   //Reading the Input file
   val input=spark.read.textFile("../Inputs_Spark/dataFile")
   
   //Importing implicit encoder to store Array[String] Instances in Dataset
   import spark.implicits._
    
   //Mapping Rdd to the case class 'InputData' which defines the schema of the Dataframe
  val inputLines =input.map(x=>x.toString().split("\\s+")).map(input=>InputData(input(0).toLong,input(1).toDouble))
   
  val inputDF=inputLines.toDF
  
  val resultData=generateResult(inputDF)
 
  writeOutput(resultData)
 
   spark.close()
}
  
  def writeOutput(resultDF:DataFrame)={
    
   val headerOptions: Map[String, String] = Map(
  ("delimiter", "\t"), 
  ("header", "true"))
 
  resultDF.coalesce(1).write
  .options(headerOptions)
  .csv("../Inputs_Spark/results")
    
  }
  
}