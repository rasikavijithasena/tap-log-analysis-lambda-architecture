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

import com.hms.tap.log.analyze.util.{ApplicationConf, InputFieldValuesConf, OutputConf, QueryConf}
import org.apache.spark.sql.SparkSession

class LogAnalyzeSparkSession {

    val appconf = new ApplicationConf()
    val inputconf = new InputFieldValuesConf()
    val queryconf = new QueryConf()
    val outputconf = new OutputConf()

    def sparkSession: SparkSession = {

        SparkSession.builder()
            .appName(appconf.getAppName())
            .master(appconf.getMaster())
            .getOrCreate()
    }
}
