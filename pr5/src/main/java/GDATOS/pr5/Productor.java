package GDATOS.pr5;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.jfree.data.time.Day;
import org.jfree.data.time.RegularTimePeriod;
import org.jfree.data.time.Second;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.time.TimeSeriesDataItem;
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
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.json.JSONObject;

public class Productor {
	
		KafkaProducer<String, String> producer;
		public static  TimeSeries hash = new TimeSeries("Hash rate");
		public static TimeSeries time = new TimeSeries("Time");
		public static TimeSeries bitcoin = new TimeSeries("Bitcoin");
		static TimeSeriesCollection dataset = new TimeSeriesCollection();

		private static void fetchDataAndUpdate() {
	        try {
	            // URL de la API de Blockchain.info
	            String urlString = "https://api.blockchain.info/stats";
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
	            long timestamp = jsonObject.getLong("timestamp");
	            double hashRate = jsonObject.getDouble("hash_rate");
	            double marketPriceUsd = jsonObject.getDouble("market_price_us");

	            Date date = new Date(timestamp);
	            Second second = new Second(date);
	            	
		        hash.add(second, hashRate);
		        bitcoin.add(second,marketPriceUsd);
	            
	            dataset.addSeries(hash);
	            dataset.addSeries(bitcoin);
	            
	            // Aquí podrías actualizar tus datos o hacer lo que necesites con ellos
	            // Por ahora, solo imprimiremos los valores actualizados
	            System.out.println("Timestamp: " + timestamp);
	            System.out.println("Tasa de hash: " + hashRate);
	            System.out.println("Bitcoin: " + marketPriceUsd);
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
		}
	    
	    public static void main(String[] args) throws InterruptedException {
	        Productor dataProducer = new Productor();
	        
	        // Check arguments length value
			if (args.length == 0) {
				System.out.println("Enter topic name");
				return;
			}
			// Assign topicName to string variable
			String topicName = args[0].toString();
			// create instance for properties to access producer configs
			Properties props = new Properties();
			// Assign localhost id
			props.put("bootstrap.servers", "localhost:9092");
			// Set acknowledgements for producer requests.
			props.put("acks", "all");
			// If the request fails, the producer can automatically retry,
			props.put("retries", 0);
			// Specify buffer size in config
			props.put("batch.size", 16384);
			// Reduce the no of requests less than 0
			props.put("linger.ms", 1);
			// The buffer.memory controls the total amount of memory available to the
			// producer for buffering.
			props.put("buffer.memory", 33554432);
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			Producer<String, String> producer = new KafkaProducer<String, String>(props);

			/*for (int i = 0; i < 20; i++) {
				producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), Integer.toString(i)));
				System.out.println("Message sent successfully");
			}*/
			while (true) {
	            fetchDataAndUpdate();
	            try {
	                // Esperar 5 minutos antes de hacer la próxima solicitud
	                Thread.sleep(3000); // 300000 milisegundos = 5 minutos
	            } catch (InterruptedException e) {
	                e.printStackTrace();
	                producer.close();
	            }
	        }   
	    }
	}

	

