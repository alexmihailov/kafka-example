package com.witcher;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Executors;

public class App {

    private static final Logger LOG = LoggerFactory.getLogger(App.class);
    private static final String TOPIC = "test";
    private static final Properties properties;
    static {
        properties = new Properties();
        try(var stream = App.class.getClassLoader().getResourceAsStream("application.properties")) {
            properties.load(stream);
        } catch (Exception e) {
            LOG.error("Error read properties", e);
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        var executor = Executors.newFixedThreadPool(2);
        var producer = new Producer(properties);
        var consumer = new Consumer(properties);
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Close app!");
            producer.close();
            consumer.close();
            executor.shutdown();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                LOG.error("Error close app!", e);
                System.exit(1);
            }
        }));
        executor.execute(producer);
        executor.execute(consumer);
    }

    private static class Producer implements Runnable, Closeable {

        private final KafkaProducer<String, String> producer;

        public Producer(Properties properties) {
            this.producer = new KafkaProducer<>(properties);
        }

        @Override
        public void run() {
            try {
                while (true) {
                    var record = new ProducerRecord<>(TOPIC, "Test key", "Test value");
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            LOG.error("Error send data!", exception);
                        } else {
                            LOG.info("Success send data!\n{}", metadata.toString());
                        }
                    });
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOG.error("Error!", e);
                    }
                }
            } catch (Exception e) {
                LOG.error("Error send data!", e);
                throw e;
            }
        }

        @Override
        public void close() {
            producer.close();
        }
    }

    private static class Consumer implements Runnable, Closeable {

        private final KafkaConsumer<String, String> consumer;

        private Consumer(Properties properties) {
            this.consumer = new KafkaConsumer<>(properties);
        }

        @Override
        public void run() {
            try {
                consumer.subscribe(Collections.singleton(TOPIC));
                while (true) {
                    for (ConsumerRecord<String, String> item : consumer.poll(Duration.ofSeconds(1))) {
                        LOG.info("Success consume data!\n{}", item.toString());
                    }
                }
            } catch (WakeupException e) {
                // nothing
            } catch (Exception e) {
                LOG.error("Error consume data!", e);
                throw e;
            } finally {
                consumer.close();
            }
        }

        @Override
        public void close() {
            consumer.wakeup();
        }
    }
}
