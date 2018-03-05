package com.hms.tap.log.analyze

import java.util.TimerTask

class BatchLayerExecution extends TimerTask{

    override def run() = {

        var batchProcessLayer = new BatchProcessingLayer();
        val sparkSession = batchProcessLayer.getSession();
        val inputString = batchProcessLayer.readFile(sparkSession);
        val splittedString = batchProcessLayer.processSelectedFields(inputString);
        val schema = batchProcessLayer.createSchema();
        val transDF = batchProcessLayer.createDF(sparkSession,splittedString,schema);
        //val outputDF = batchProcessLayer.aggregateDF(transDF);
        transDF.show();
        batchProcessLayer.saveData(transDF);
    }

    /*def main(args: Array[String]): Unit = {

        BatchLayerExecution.run()
    }*/
}
