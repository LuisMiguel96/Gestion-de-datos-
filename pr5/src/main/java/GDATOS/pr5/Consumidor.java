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

    public static TimeSeries hash = new TimeSeries("Hash rate");
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
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        XYDataset datasetXY = dataset;
        JFreeChart chart = createChart(datasetXY);
        ChartPanel chartPanel = new ChartPanel(chart, false);
        chartPanel.setPreferredSize(new java.awt.Dimension(500, 270));
        chartPanel.setMouseZoomable(true, false);
        setContentPane(chartPanel);
        dataset.addSeries(hash);
        dataset.addSeries(bitcoin);

        // Crear y ejecutar hilos para los consumidores de Bitcoin y Hash-rate
        Thread bitcoinThread = new Thread(new BitcoinConsumer(props));
        Thread hashRateThread = new Thread(new HashRateConsumer(props));

        bitcoinThread.start();
        hashRateThread.start();
    }

    private static JFreeChart createChart(XYDataset dataset) {
        JFreeChart chart = ChartFactory.createTimeSeriesChart(
            "Bitcoin price vs Hash Rate evolution",  // title
            "Time",             // x-axis label
            "Hash-rate",        // y-axis label
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
       
        NumberAxis bitcoinAxis = new NumberAxis("Bitcoin");
        plot.setRangeAxis(1, bitcoinAxis);
        TimeSeriesCollection bitcoinDataset = new TimeSeriesCollection();
        bitcoinDataset.addSeries(bitcoin);
        plot.setDataset(1, bitcoinDataset);
        plot.mapDatasetToRangeAxis(1, 1);
        XYItemRenderer bitcoinRenderer = new XYLineAndShapeRenderer();
        plot.setRenderer(1, bitcoinRenderer);
        chart.getXYPlot().getRenderer().setSeriesVisibleInLegend(1, false);
        
        return chart;
    }
    
    public static JPanel createDemoPanel() {
        JFreeChart chart = createChart(dataset);
        return new ChartPanel(chart);
    }

    public static void main(String[] args) {
        String title = "";
        Consumidor dataConsumer = new Consumidor(title);
        dataConsumer.pack();
        dataConsumer.setVisible(true);
    }
}

class BitcoinConsumer implements Runnable {
    private final Properties props;

    public BitcoinConsumer(Properties props) {
        this.props = props;
    }

    @Override
    public void run() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("Bitcoin"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                //System.out.println("Número de registros recibidos: " + records.count());
                records.forEach(record -> {
                	String value = record.value();
                	if (!value.isEmpty()) { // Verificar si la cadena no está vacía
                        try {
                            System.out.println("Received Bitcoin price: " + value);
                            Second currentSecond = new Second();
                            try {
                            	Consumidor.bitcoin.addOrUpdate(currentSecond, Double.valueOf(value));
                        	}catch (IllegalArgumentException e) {
                        	System.err.println("Valor fuera de rango: " + value);
                            e.printStackTrace();
                        	}
                        } catch (NumberFormatException e) {
                            System.err.println("Error converting value to double: " + value);
                            e.printStackTrace();
                        }
                    }
                });
            }
        }
    }
}

class HashRateConsumer implements Runnable {
    private final Properties props;
    public HashRateConsumer(Properties props) {
        this.props = props;
    }

    @Override
    public void run() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("Hash-rate"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                //System.out.println("Número de registros recibidos: " + records.count());
                records.forEach(record -> {
                	String value = record.value();
                	if (!value.isEmpty()) { // Verificar si la cadena no está vacía
                        try {
                            System.out.println("Received Hash rate data: " + value);
                            Second currentSecond = new Second();
                            try {
                            	Consumidor.hash.addOrUpdate(currentSecond, Double.valueOf(value));
                            }catch (IllegalArgumentException e) {
                            	System.err.println("Valor fuera de rango: " + value);
                                e.printStackTrace();
                            }
                            	
                        } catch (NumberFormatException e) {
                            System.err.println("Error converting value to double: " + value);
                            e.printStackTrace();
                        }
                    }
                	else {
                		System.out.println("CADENA VACIA");
                	}
                });
            }
        }
    }
}
