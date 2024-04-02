package GDATOS.pr5;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class ProducerBitcoin {
	

	    private KafkaProducer<String, String> producer;

	    public ProducerBitcoin() {
	        Properties props = new Properties();
	        props.put("bootstrap.servers", "localhost:9092");
	        props.put("acks", "all");
	        props.put("retries", 0);
	        props.put("batch.size", 16384);
	        props.put("linger.ms", 1);
	        props.put("buffer.memory", 33554432);
	        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	        
	        this.producer = new KafkaProducer<String, String>(props);
	    }

	    public void produceBitcoinData() throws InterruptedException {
	        Random rand = new Random();
	        while (true) {
	            double bitcoinPrice = 50000 + (1000 * rand.nextDouble()); // Simula el precio de Bitcoin
	            double hashRate = 150 + (10 * rand.nextDouble()); // Simula el hash rate
	            String data = "{\"bitcoin_price\": " + bitcoinPrice + ", \"hash_rate\": " + hashRate + "}";
	            //producer.send();
	            Thread.sleep(1000); // Espera 1 segundo antes de enviar el siguiente dato
	        }
	    }

	    public void closeProducer() {
	        producer.close();
	    }

	    public static void main(String[] args) {
	        ProducerBitcoin dataProducer = new ProducerBitcoin();
	        try {
	            dataProducer.produceBitcoinData();
	        } catch (InterruptedException e) {
	            e.printStackTrace();
	        } finally {
	            dataProducer.closeProducer();
	        }
	    }
	}

	

