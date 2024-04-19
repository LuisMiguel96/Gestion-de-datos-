package GDATOS.pr5;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import org.json.JSONObject;
public class ProducerHashRate {
	
		private static KafkaProducer<String, String> producer;
		public static TimeSeries hash = new TimeSeries("Hash rate");
		public static TimeSeries time = new TimeSeries("Time");
		static TimeSeriesCollection dataset = new TimeSeriesCollection();

		private static void fetchDataAndUpdate(String topicName) {
	        try {
	            // URL de la API de Blockchain.info
	            String urlString = "https://api.coindesk.com/v1/bpi/currentprice.json";
	            URL url = new URL(urlString);
	            
	            // Realizar la conexión HTTP
	            HttpURLConnection con = (HttpURLConnection) url.openConnection();
	            con.setRequestMethod("GET");
	            con.setRequestProperty("Cache-Control", "no-cache");
	            
	            // Leer la respuesta
	            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
	            String inputLine;
	            StringBuilder response = new StringBuilder();
	            while ((inputLine = in.readLine()) != null) {
	                response.append(inputLine);
	            }
	            in.close();
	            
	            // Parsear la respuesta JSON
	            JSONObject jsonObject = new JSONObject(response.toString());
	            
	            // Acceder a los valores específicos
	            String timestamp = jsonObject.getJSONObject("time").getString("updated");
	            Double hashRate = jsonObject.getJSONObject("bpi").getJSONObject("USD").getDouble("rate_float");
	            
	            dataset.addSeries(hash);
	            dataset.addSeries(time);

	            String message = "Timestamp: " + timestamp + ", Hash Rate: " + hashRate;
	            System.out.println("Mensaje enviado al tema de Kafka: " + message);
	            String aux = hashRate.toString();
	           
	            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, aux);
	            producer.send(record);
	            Thread.sleep(1000);
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
		}
		private static void initializeProducer() {
			Properties props = new Properties();
			props.put("bootstrap.servers", "localhost:9092");
			props.put("acks", "all");
			props.put("retries", 0);
			props.put("batch.size", 16384);
			props.put("linger.ms", 1);
			props.put("buffer.memory", 33554432);
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			producer = new KafkaProducer<>(props);
	    }
	    
	    public static void main(String[] args) throws InterruptedException {
	        ProducerHashRate dataProducer = new ProducerHashRate();
	        
	        // Check arguments length value
			if (args.length == 0) {
				System.out.println("Enter topic name");
				return;
			}
			// Assign topicName to string variable
			String topicName = args[0].toString();
			initializeProducer(); 
			
			while(true) {
				fetchDataAndUpdate(topicName);
			}
	    }
	}