
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerDemo {

    public static void main(String[] args) {
        Properties properties = configureKafkaProperties();

        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList("second_topic"));

        while (true){
            ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(100);
            for(ConsumerRecord<String,String> consumerRecord:consumerRecords){
                printConsumerRecord(consumerRecord);
            }
        }

    }

    private static void printConsumerRecord(ConsumerRecord<String, String> consumerRecord) {
        consumerRecord.value();
        consumerRecord.key();
        consumerRecord.offset();
        consumerRecord.partition();
        consumerRecord.topic();
        consumerRecord.timestamp();
        System.out.println("Partition: "+consumerRecord.partition()+", Offset: "+consumerRecord.offset()
                +",Key :"+consumerRecord.key()+", Value: "+consumerRecord.value());
    }

    private static Properties configureKafkaProperties() {
        Properties properties = new Properties();

        //kafka bootstrap server
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:7092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty("group.id","test");
        properties.setProperty("enable.auto.commit","true");
        properties.setProperty("auto.commit.interval.ms","1000");
        properties.setProperty("auto.offset.reset","earliest");
        return properties;
    }
}
