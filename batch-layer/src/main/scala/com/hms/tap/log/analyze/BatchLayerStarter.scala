package com.hms.tap.log.analyze

import java.util.Timer

object BatchLayerStarter {

    def main(args: Array[String]): Unit = {

        //val timer = new Timer()
        val batchLayerExecution = new BatchLayerExecution
        batchLayerExecution.run()

        //timer.scheduleAtFixedRate(batchLayerExecution, 0, 5000)
    }
}
