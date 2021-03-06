package com.hms.tap.log.analyze

import java.io.IOException
import java.util.TimerTask

import com.hms.tap.log.analyze.util.{ApplicationConf, OutputConf}
import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.util.Properties

class ServingLayer extends TimerTask{

    var batchLayerImpl = new BatchProcessingLayer()
    var speedLayerImpl = new SpeedLayerImpl()
    var appconf = new ApplicationConf()
    var outputconf = new OutputConf();

    def getSparkSession(): SparkSession ={
        val appName = appconf.getAppName();
        val master = appconf.getMaster();
        val hiveDir = appconf.getHiveConf();
        val thrift = appconf.getThriftConf();

        val spark = SparkSession
            .builder
            .appName(appName)
            .master(master)
            .config("spark.sql.warehouse.dir", hiveDir)
            .config("hive.metastore.uris", thrift)
            .enableHiveSupport()
            .getOrCreate()

        spark;
    }

    def getBatchDF(spark : SparkSession): DataFrame ={

        var batchPath = outputconf.batchOutputPath;
        var batchDF = spark.read
            .parquet(batchPath)

        batchDF;
    }

    def getSpeedDF(spark : SparkSession): DataFrame ={

        var structuredPath = outputconf.structuredPath;
        var speedDF = spark.read
            .parquet(structuredPath)

        speedDF;
    }


    def getMergeDF(batchDF : DataFrame , speedDF :DataFrame): DataFrame ={
        var mergedDF = batchDF.union(speedDF)
        print(mergedDF.isStreaming)
        mergedDF;

    }

    def saveDatasetToHive(spark:SparkSession , mergedDF : DataFrame): Unit ={
        mergedDF.createOrReplaceTempView("mergedDataset")
        spark.sql("CREATE TABLE IF NOT EXISTS knowage_report(" +
            "time_stamp String, sp_id String, ncs String, operator_name String, sp_revenue String, total_amount_sp String)")
        spark.sql("INSERT OVERWRITE TABLE knowage_report  SELECT * FROM mergedDataset");
        print("finish");
    }

    def saveDatasetToMysql(spark:SparkSession , mergedDF : DataFrame): Unit ={
        val jdbcUsername = "root"
        val jdbcPassword = "cloudera"
        val jdbcHostname = "localhost"
        val jdbcPort = 3306
        val jdbcDatabase ="summary"
        val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}&password=${jdbcPassword}"

        // Option 2: Create a Properties() object to hold the parameters. You can create the JDBC URL without passing in the user/password parameters directly.
        val connectionProperties = new Properties()
        connectionProperties.put("user", "root")
        connectionProperties.put("password", "cloudera")

        mergedDF.createOrReplaceTempView("mergedDataset")
        spark.table("mergedDataset").write
        .mode(SaveMode.Overwrite).jdbc(jdbcUrl, "knowage_report", connectionProperties)

        //spark.sql("CREATE TABLE IF NOT EXISTS knowage_report(" +
        //  "time_stamp String, sp_id String, ncs String, operator_name String, sp_revenue String, total_amount_sp String)")
        //spark.sql("INSERT OVERWRITE TABLE knowage_report  SELECT * FROM mergedDataset");
        print("finish");
    }


    def gethiveDataset(spark:SparkSession): DataFrame ={
        var hiveDataset = spark.sql("select * from knowage_report")
        hiveDataset;
    }

    def streamOutput(mergedDF : DataFrame): Unit ={

        print(mergedDF.isStreaming)
        val outputDF = mergedDF.writeStream
            .outputMode("append")
            .format("console")
            //.option("path" , "/home/cloudera/Desktop/par")
            .option("checkpointLocation" , "/home/cloudera/Documents/checkpoints")
            .start()
        outputDF.awaitTermination();
    }

    override def run() = {

        try {
            val servingLayerImpl = new ServingLayer()
            val spark = servingLayerImpl.getSparkSession()
            val batchDF = servingLayerImpl.getBatchDF(spark)
            val speedDF = servingLayerImpl.getSpeedDF(spark)
            //batchDF.show();
            //speedDF.show();
            val mergedDF = servingLayerImpl.getMergeDF(batchDF, speedDF)
            //mergedDF.show(1000);
            servingLayerImpl.saveDatasetToHive(spark,mergedDF)
        }catch {
            case sparkException: SparkException => sparkException.printStackTrace()
            case ioException: IOException => ioException.printStackTrace()
            case exception: Exception => exception.printStackTrace()
        }

    }
}
