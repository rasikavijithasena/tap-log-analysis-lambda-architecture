package com.hms.tap.log.analyze.util

import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.StringDeserializer

class KafkaConf {

    private var conf = ConfigFactory.parseResources("TypesafeConfig.conf")

    var topic = conf.getString("kafka.topicSet")
    var delimeter = conf.getString("kafka.topicSetSeperator")
    var bootstrap = conf.getString("kafka.bootstrapServers")
    var brokers = conf.getString("kafka.brokers")

    def getTopic(): String = {
        topic
    }

    def getBootstrap(): String = {
        bootstrap
    }

    val topics = Array("topicA", "topicB")

    val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "use_a_separate_group_id_for_each_stream",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
    )


}
