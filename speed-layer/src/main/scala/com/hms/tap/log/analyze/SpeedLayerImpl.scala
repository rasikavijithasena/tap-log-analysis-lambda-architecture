package com.hms.tap.log.analyze

import java.io.IOException
import java.lang.Exception
import java.sql.Timestamp
import java.util.TimerTask

import com.hms.tap.log.analyze.util._
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.control.Exception

/**
  * This implements the speed layer of the log analyzing application
  * */

class SpeedLayerImpl extends TimerTask{

    //val logger = new Logger(this)

    val kafkaconf = new KafkaConf()
    val inputconf = new InputFieldValuesConf()
    val queryconf = new QueryConf()
    val appconf = new ApplicationConf()
    val outputconf = new OutputConf();


    def createSparkSession(): SparkSession = {

      val appName= appconf.getAppName()
      val master = appconf.getMaster()

      val spark = SparkSession
        .builder
        .appName(appName)
        .master(master)
        .getOrCreate()

        spark
    }

    def getInputDataset(spark: SparkSession): Dataset[(String, Timestamp)] = {

        val bootstrap = kafkaconf.getBootstrap()
        val topic = kafkaconf.getTopic()
        val speedLayerImpl = new SpeedLayerImpl()

        import spark.implicits._


        val inputDF = speedLayerImpl.createSparkSession()
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", topic)
            .option("includeTimestamp", true)
            .load()
            .selectExpr("CAST(value AS STRING)", "timestamp")
            .as[(String, Timestamp)]

        inputDF
    }

    def splitFields(inputDF: Dataset[(String, Timestamp)]): DataFrame = {

        val num = inputconf.getFieldCount()
        //val arrayNames1 = Array("timestamp", "name", "age", "address")
        val fieldNames = inputconf.getFieldsAsArray()
        val delimeter = inputconf.getDelimeter()

        val splittedDF = inputDF.withColumn("temp", split(col("value"), delimeter)).select(
            col("timestamp") +: (0 until num).map(i => col("temp").getItem(i).as(s"col$i")): _*
        )
            .toDF(fieldNames: _*)

        splittedDF

    }

    def groupDataset(splittedDF: DataFrame): DataFrame = {

        val groupingFieldsAsArray = queryconf.getGroupingFieldsAsArray()
        var selectedFieldsAsArray = queryconf.getSelectedFieldsAsArray()

        val groupedDF = splittedDF
            .withWatermark("timestamp", "1 seconds")
            .select(col("timestamp"), substring(col(selectedFieldsAsArray.apply(0)), 0, 10).as(selectedFieldsAsArray.apply(0)),
                col(selectedFieldsAsArray.apply(1)), col(selectedFieldsAsArray.apply(2)), col(selectedFieldsAsArray.apply(3)))
            .filter(queryconf.getFilterString())
            .groupBy("timestamp", queryconf.getGroupingFields(0), queryconf.getGroupingFields(1))
            .count()


        val selectedDF = groupedDF.select("time_stamp", "app_id", "count").toDF()
        selectedDF
    }

    def selectDataset(splittedDF : DataFrame): DataFrame ={
        var selectedFieldsAsArray = queryconf.getSelectedFieldsAsArray();

        val outputDF = splittedDF.select(substring(col(selectedFieldsAsArray.apply(0)) , 0 ,10).as(selectedFieldsAsArray.apply(0)) ,
            col(selectedFieldsAsArray.apply(1)) , col(selectedFieldsAsArray.apply(2)) , col(selectedFieldsAsArray.apply(3)),
            col(selectedFieldsAsArray.apply(4)), col(selectedFieldsAsArray.apply(5)))
          .toDF()

        outputDF;
    }

    def writeDataset(groupedDF : DataFrame): Unit ={

        val path = outputconf.structuredPath;
        val checkPath = outputconf.structuredCheckpointPath;

        /*val outputDF = groupedDF.writeStream
          .outputMode("complete")
          .format("console")
          .start()*/
        val outputDF = groupedDF.writeStream
          .outputMode("append")
          .format("parquet")
          .option("path" , path)
          .option("checkpointLocation" , checkPath)
          .start()
        outputDF.awaitTermination();

    }


    override def run() = {

        val speedLayerImpl =  new SpeedLayerImpl

        val sparkSession = speedLayerImpl.createSparkSession()
        val inputDF = speedLayerImpl.getInputDataset(sparkSession)
        val splittedDF = speedLayerImpl.splitFields(inputDF)
        //val groupedDF = speedLayerImpl.groupDataset(splittedDF)
        val selectedDF = speedLayerImpl.selectDataset(splittedDF);
        speedLayerImpl.writeDataset(selectedDF);
    }
}


