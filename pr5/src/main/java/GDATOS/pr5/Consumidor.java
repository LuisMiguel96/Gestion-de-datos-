package GDATOS.pr5;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.chart.ui.ApplicationFrame;
import org.jfree.chart.ui.RectangleInsets;
import org.jfree.data.time.Second;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;
import java.awt.Color;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import javax.swing.JPanel;

public class Consumidor extends ApplicationFrame {

    private final KafkaConsumer<String, String> consumer;
    public static  TimeSeries hash = new TimeSeries("Hash rate");
	public static TimeSeries time = new TimeSeries("Time");
	public static TimeSeries bitcoin = new TimeSeries("Bitcoin");
	static TimeSeriesCollection dataset = new TimeSeriesCollection();


    public Consumidor(String title) {
    	 super(title);
         Properties props = new Properties();
         String topicName1 = "Hash-rate";
         String topicName = "Bitcoin";
         props.put("bootstrap.servers", "localhost:9092");
         props.put("group.id", "test");
         props.put("enable.auto.commit", "true");
         props.put("auto.commit.interval.ms", "1000");
         props.put("session.timeout.ms", "30000");
         props.put("key.deserializer", 
            "org.apache.kafka.common.serialization.StringDeserializer");
         props.put("value.deserializer", 
            "org.apache.kafka.common.serialization.StringDeserializer");
         KafkaConsumer<String, String> consumer = new KafkaConsumer <String, String>(props);
         
         XYDataset datasetXY = dataset;
         JFreeChart chart = createChart(datasetXY);
         ChartPanel chartPanel = new ChartPanel(chart, false);
         chartPanel.setPreferredSize(new java.awt.Dimension(500, 270));
         chartPanel.setMouseZoomable(true, false);
         setContentPane(chartPanel);
         //dataset.addSeries(bitcoin);
         dataset.addSeries(hash);
         
         //Kafka Consumer subscribes list of topics here.
         consumer.subscribe(Arrays.asList(topicName));
         consumer.subscribe(Arrays.asList(topicName1));
         
         //print the topic name
         System.out.println("Subscribed to topic " + topicName);
         System.out.println("Subscribed to topic " + topicName1);

        this.consumer = new KafkaConsumer<>(props);
    }

    public void subscribeToBitcoinDataTopic() {
        consumer.subscribe(Collections.singletonList("Bitcoin"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> {
                System.out.println("Received Bitcoin data: " + record.value());
                Second currentSecond = new Second();
                bitcoin.add(currentSecond, Double.valueOf(record.value()));
                dataset.addSeries(bitcoin);
            });
        }
    }
    
    public void subscribeToHashRateDataTopic() {
        consumer.subscribe(Collections.singletonList("Hash-rate"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> {
                System.out.println("Received Hash rate data: " + record.value());
                Second currentSecond = new Second();
                hash.add(currentSecond, Double.valueOf(record.value()));
                dataset.addSeries(hash);
            });
        }
    }

    private static JFreeChart createChart(XYDataset dataset) {

        JFreeChart chart = ChartFactory.createTimeSeriesChart(
            "Bitcoin price vs Hash Rate evolution",  // title
            "Time",             // x-axis label
            "Hash-rate",   		// y-axis label
            dataset,            // data
            true,               // create legend?
            true,               // generate tooltips?
            false               // generate URLs?
        );

        chart.setBackgroundPaint(Color.white);

        XYPlot plot = chart.getXYPlot();
        plot.setBackgroundPaint(Color.lightGray);
        plot.setDomainGridlinePaint(Color.white);
        plot.setRangeGridlinePaint(Color.white);
        plot.setAxisOffset(new RectangleInsets(5.0, 5.0, 5.0, 5.0));
        plot.setDomainCrosshairVisible(true);
        plot.setRangeCrosshairVisible(true);
       
     // Configurar el eje y secundario para la serie de Bitcoin
        NumberAxis bitcoinAxis = new NumberAxis("Bitcoin");
        plot.setRangeAxis(1, bitcoinAxis); // Configurar el eje y secundario

        // Crear un conjunto de datos separado para la serie de Bitcoin
        TimeSeriesCollection bitcoinDataset = new TimeSeriesCollection();
        bitcoinDataset.addSeries(bitcoin);
        plot.setDataset(1, bitcoinDataset); // Asignar el conjunto de datos de Bitcoin al eje y secundario

        // Asignar la serie de Bitcoin al eje y secundario
        plot.mapDatasetToRangeAxis(1, 1);

        // Crear un renderer para la serie de Bitcoin
        XYItemRenderer bitcoinRenderer = new XYLineAndShapeRenderer();
        plot.setRenderer(1, bitcoinRenderer); // Asignar el renderer a la serie de Bitcoin

        // Ocultar la leyenda de la serie de Bitcoin en el eje y principal
        chart.getXYPlot().getRenderer().setSeriesVisibleInLegend(1, false);

        
        /*XYItemRenderer r = plot.getRenderer();
        if (r instanceof XYLineAndShapeRenderer) {
            XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) r;
            renderer.setDefaultShapesVisible(true);
            renderer.setDefaultShapesFilled(true);
        } 
        DateAxis axis = (DateAxis) plot.getDomainAxis();
        axis.setDateFormatOverride(new SimpleDateFormat("MMM-yyyy"));  
        //chart.addSeries(bitcoin);*/
        return chart;

    }
    
    public static JPanel createDemoPanel() {
        JFreeChart chart = createChart(dataset);
        return new ChartPanel(chart);
    }
    public void closeConsumer() {
        consumer.close();
    }

    public static void main(String[] args) {
    	String title = new String();
        Consumidor dataConsumer = new Consumidor(title);
        dataConsumer.pack();
        //RefineryUtilities.centerFrameOnScreen(dataConsumer);
        dataConsumer.setVisible(true);
        dataConsumer.subscribeToBitcoinDataTopic();
        dataConsumer.subscribeToHashRateDataTopic();
        
    }
}
