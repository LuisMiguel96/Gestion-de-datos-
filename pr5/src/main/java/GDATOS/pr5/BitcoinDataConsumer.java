package GDATOS.pr5;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class BitcoinDataConsumer {

    private final KafkaConsumer<String, String> consumer;

    public BitcoinDataConsumer() {
         Properties props = new Properties();
         String topicName = "Bitcoin";
         props.put("bootstrap.servers", "localhost:9092");
         props.put("group.id", "test");
         props.put("enable.auto.commit", "true");
         props.put("auto.commit.interval.ms", "1000");
         props.put("session.timeout.ms", "30000");
         props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
         props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
         KafkaConsumer<String, String> consumer = new KafkaConsumer
            <String, String>(props);
         
         //Kafka Consumer subscribes list of topics here.
         consumer.subscribe(Arrays.asList(topicName));
         
         //print the topic name
         System.out.println("Subscribed to topic " + topicName);

        this.consumer = new KafkaConsumer<>(props);
    }

    public void subscribeToBitcoinDataTopic() {
        consumer.subscribe(Collections.singletonList("Bitcoin"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> {
                System.out.println("Received Bitcoin data: " + record.value());
                // Aquí puedes realizar cualquier procesamiento adicional que desees con los datos recibidos
            });
        }
    }

    public void closeConsumer() {
        consumer.close();
    }

    public static void main(String[] args) {
        BitcoinDataConsumer dataConsumer = new BitcoinDataConsumer();
        dataConsumer.subscribeToBitcoinDataTopic();
    }
}
