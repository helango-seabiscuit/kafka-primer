package practice;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Properties;


public class SimpleProducer {

	private static final String topicName = "replicated-kafka";
	public static void main(String[] args) {
		consume();
	}

	public static void produce(){
		Properties properties = new Properties();
		//kafka bootstrap server
		properties.setProperty("bootstrap.servers","127.0.0.1:9093");
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer",StringSerializer.class.getName());
		//producer acks
		properties.setProperty("acks","1");
		properties.setProperty("retries","3");
		properties.setProperty("linger.ms","1");

		Producer<String,String> producer = new KafkaProducer<String, String>(properties);

		for (int i=0;i<8;i++){
			ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topicName,Integer.toString(i),"message "+i);
			producer.send(producerRecord);
		}
		producer.close();
	}

	public static void consume(){
		Properties properties = new Properties();
		//kafka bootstrap server
		properties.setProperty("bootstrap.servers","localhost:9093,localhost:9094");
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());
		properties.setProperty("key.deserializer", StringDeserializer.class.getName());
		properties.setProperty("value.deserializer",StringDeserializer.class.getName());
		//producer acks
		properties.setProperty("group.id","test");
		properties.setProperty("enable.auto.commit","true");
		properties.setProperty("auto.commit.interval.ms","1000");
		properties.setProperty("auto.offset.reset","earliest");

		KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
		kafkaConsumer.subscribe(Arrays.asList(topicName));

		while (true){
			ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(100);
			for(ConsumerRecord<String,String> consumerRecord:consumerRecords){
				System.out.println(consumerRecord.value());
			}
		}
	}

}

