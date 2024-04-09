package GDATOS.pr5;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class HashRateConsumer {

    private final KafkaConsumer<String, String> consumer;

    public HashRateConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Hash-rate");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        this.consumer = new KafkaConsumer<>(props);
    }

    public void subscribeToBitcoinDataTopic() {
        consumer.subscribe(Collections.singletonList("Hash-Rate"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> {
                System.out.println("Received Hash rate data: " + record.value());
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
