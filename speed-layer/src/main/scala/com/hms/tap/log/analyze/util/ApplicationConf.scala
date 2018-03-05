package com.hms.tap.log.analyze.util

import com.typesafe.config.ConfigFactory

/**
  * Contains the necessary configurations that are needed to run the application
  * Reads the configuration file*/

class ApplicationConf extends Serializable {

    private val conf = ConfigFactory.parseResources("TypesafeConfig.conf")

    private val appName = conf.getString("application.appName")
    private val master = conf.getString("application.master")
    private val duration = conf.getInt("application.duration")
    private val hiveConf = conf.getString("application.hiveConf")
    private val thriftConf = conf.getString("application.thriftConf")
    private val shuffle = conf.getInt("application.shuffle")
    private val parellelismPartitions = conf.getInt("application.parellelismPartitions")
    private val checkpointDirectory = conf.getString("application.checkpointDirectory")

    def getAppName(): String = {
        appName
    }

    def getMaster(): String = {
        master
    }

    def getDuration(): Int = {
        duration
    }

    def getHiveConf(): String = {
        hiveConf
    }

    def getThriftConf(): String = {
        thriftConf
    }

    def getShuffle(): Int = {
        shuffle
    }

    def getParallelism(): Int = {
        parellelismPartitions
    }

    def getCheckpointDir(): String = {
        checkpointDirectory
    }

}
