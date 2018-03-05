package com.hms.tap.log.analyze

import java.util.Timer

object SpeedLayerExecutor {

    def main(args: Array[String]): Unit = {

        //val timer = new Timer()

        val speedLayerImpl = new SpeedLayerImpl()
        speedLayerImpl.run()

        //timer.scheduleAtFixedRate(speedLayerImpl, 0, 1000)
    }
}
