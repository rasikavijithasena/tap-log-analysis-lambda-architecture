/*
 * (C) Copyright 2010-2018 hSenid Mobile Solutions (Pvt) Limited.
 * All Rights Reserved.
 *
 * These materials are unpublished, proprietary, confidential source code of
 * hSenid Mobile Solutions (Pvt) Limited and constitute a TRADE SECRET
 * of hSenid Mobile Solutions (Pvt) Limited.
 *
 * hSenid Mobile Solutions (Pvt) Limited retains all title to and intellectual
 * property rights in these materials.
 */

package com.hms.tap.log.analyze

import java.io.File

import com.hms.tap.log.analyze.util.{ApplicationConf, InputFieldValuesConf, OutputConf, QueryConf}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


/**
  * Batch processing layer implementation
  * */

class BatchProcessingLayer extends Serializable{

    val appconf = new ApplicationConf();
    var inputconf = new InputFieldValuesConf();
    val queryconf = new QueryConf();
    val outputconf = new OutputConf();

    def getSession(): SparkSession ={
        val appName = appconf.getAppName();
        val master = appconf.getMaster();
        val hiveDir = appconf.getHiveConf();
        val thrift = appconf.getThriftConf();
        val shufflePartitions = appconf.getShuffle();
        val parallelism = appconf.getParallelism();
        val warehouseLocation = new File("spark-warehouse").getAbsolutePath

        val conf = new SparkConf().setAppName(appName).setMaster(master)
        conf.set("spark.network.timeout", "600s")
        //conf.set("spark.driver.allowMultipleContexts" , "true")


        new SparkContext(conf)

        val spark = SparkSession
          .builder()
          .appName(appName)
          .config("spark.sql.warehouse.dir", hiveDir)
          .config("hive.metastore.uris", thrift)
          //.config("spark.sql.warehouse.dir", warehouseLocation)
          .config("spark.sql.shuffle.partitions", shufflePartitions)
          .config("spark.default.parallelism" , parallelism)
          .enableHiveSupport()
          .getOrCreate()

        spark;
    }

    def readFile(spark : SparkSession): RDD[String] ={

        val path = inputconf.getDirectory();

        //val transRDD = spark.sparkContext.textFile("/home/cloudera/Desktop/logs.txt")
        val transRDD = spark.sparkContext.textFile(path)

        transRDD;
    }

    def processAllFields(transRDD : RDD[String]): RDD[Row] ={
        val rowRDD = transRDD
          .map(_.split("\\|" , -1))
          .map(attributes => Row.fromSeq(attributes))

        rowRDD;
    }

    def processSelectedFields(transRDD : RDD[String]): RDD[Row] ={

        val delimeter = inputconf.getDelimeter();
        val numberOfSelectedFields = inputconf.getNumberOfSelectedFields();


        val selectedFieldsRDD =  transRDD
          .map(_.split(delimeter , -1))
          .map(x => {

              val trans:Array[String] = new Array[String](numberOfSelectedFields)
              val counter = 0;
              //println(x(0));


              val array:Array[Int] = Array(0,1,2)

              for(i <- counter until numberOfSelectedFields) {

                  val fieldNumber = inputconf.getFieldNumber(i);
                  val substringValue = inputconf.getSubString(i);


                  if(substringValue == null){
                      trans(i) = x(fieldNumber-1);
                  } else {

                      trans(i) = x(fieldNumber-1).substring(substringValue.apply(0), substringValue.apply(1));
                  }

              }

              Row.fromSeq(trans)

          })

        selectedFieldsRDD;
    }

    def createSchema(): StructType ={
        val schemaString = inputconf.getSchema();
        val schemadelimeter = inputconf.getFieldSeperator();

        val fields = schemaString.split(schemadelimeter)
          .map(fieldName => StructField(fieldName, StringType, nullable = true))
        val schema = StructType(fields)

        schema;
    }

    def createDF(spark: SparkSession,rowRDD :RDD[Row], schema:StructType): DataFrame ={

        val transDF = spark.createDataFrame(rowRDD, schema)
        //transDF.checkpoint();

        transDF;

    }

    def aggregateDF(transDF : DataFrame): DataFrame ={
        val filteredDF = transDF.filter(queryconf.getFilterString());
        val groupedDF = filteredDF.groupBy(queryconf.getGroupingFields(0), queryconf.getGroupingFields(1)).count();
        groupedDF.show();

        groupedDF;
    }

    def saveData(outputDF : DataFrame): Unit ={
        val path = outputconf.batchOutputPath;

        outputDF.write
          .mode(SaveMode.Overwrite)
          .format("parquet")
          .save(path)
    }


}
