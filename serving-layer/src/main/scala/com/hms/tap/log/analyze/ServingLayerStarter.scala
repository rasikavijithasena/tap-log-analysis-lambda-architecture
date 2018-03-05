package com.hms.tap.log.analyze

import java.util.Timer

object ServingLayerStarter {

    def main(args: Array[String]): Unit = {

        val timer = new Timer()
        val servingLayer = new ServingLayer()
	//servingLayer.run();
        timer.scheduleAtFixedRate(servingLayer, 0,10000)
    }
}
