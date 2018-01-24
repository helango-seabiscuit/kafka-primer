import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerDemo {

    public static void main(String[] args) {
        Properties properties = configureKafkaProperties();

        Producer<String,String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);

        for (int key=0;key<10;key++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("second_topic",
                    Integer.toString(key), "message test for key:"+key);
            producer.send(producerRecord);
        }

        producer.close();

        //topic name
    }

    private static Properties configureKafkaProperties() {
        Properties properties = new Properties();

        //kafka bootstrap server
        properties.setProperty("bootstrap.servers","127.0.0.1:7092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());
        //producer acks
        properties.setProperty("acks","1");
        properties.setProperty("retries","3");
        properties.setProperty("linger.ms","1");
        return properties;
    }
}
