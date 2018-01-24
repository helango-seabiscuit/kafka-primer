package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class ProducerCount {

    public static void main(String[] args) {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"wordcount-live-test");
        streamsConfiguration.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        streamsConfiguration.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName());
       // streamsConfiguration.setProperty(StreamsConfig.STATE_DIR_CONFIG,"/kafka_stream_state");

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String,String> textLines = builder.stream("inputTopic");

        KTable<String,Long> wordCounts = textLines.flatMapValues(values-> Arrays.asList(values.split(" ")))
                .groupBy((key,word)-> word)
                .count();

        wordCounts.foreach((w,c)-> System.out.println("word: "+w +" -> "+c));

        KafkaStreams streams = new KafkaStreams(builder,streamsConfiguration);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
