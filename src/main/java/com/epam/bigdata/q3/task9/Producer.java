package com.epam.bigdata.q3.task9;

import com.google.common.io.Resources;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Stream;


public class Producer {
	public static final String TOPIC = "logs-topic";
	public static final String SEND = "SEND: ";
	public static final String ISO = "ISO-8859-1";	
	public static final String PROPERTIES = "producer.props";	
	public static final String PARAMS_ERROR = "Usage: producer <file_path>";
	
    public static void main(String[] args) throws IOException {
    	
        // Set up the producer
        KafkaProducer<String, String> producer;
        try (InputStream props = Resources.getResource(PROPERTIES).openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);
        }

        try(Stream<java.nio.file.Path> paths = Files.walk(Paths.get(args[1]))) {
            paths.forEach(filePath -> {
                if (Files.isRegularFile(filePath)) {
                    try(Stream<String> lines = Files.lines(filePath, Charset.forName(ISO))) {
                        lines.forEach(line ->{
                            producer.send(new ProducerRecord<>(TOPIC, line));
                            System.out.println(SEND + line);
                        });
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            });
        } finally {
        	System.out.println("FINISH!");
            producer.close();
        }
    }
}
