package com.bigdata

import java.util.Properties

import kafka.utils.Logging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

class SimpleProducer(val topic: String) extends Logging {

  val props: Properties = createProducerConfig()
  val producer = new KafkaProducer[String, String](props)

  def createProducerConfig(): Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.RETRIES_CONFIG, 0)
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384)
    props.put(ProducerConfig.LINGER_MS_CONFIG, 1)
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

  def run(): Unit = {
    for (i <- 1 to 10) {
      producer.send(new ProducerRecord[String, String](this.topic, Integer.toString(i), Integer.toString(i)))
    }

    System.out.println("Message sent successfully")
    producer.close()
  }

}

object SimpleProducer extends App {
  if (args.length == 0) {
    System.out.println("Enter topic name")
  } else {
    val app = new SimpleProducer(args(0))
    app.run()
  }
}
