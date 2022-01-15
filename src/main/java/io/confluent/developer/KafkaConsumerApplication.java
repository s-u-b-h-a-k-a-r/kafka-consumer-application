package io.confluent.developer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
@RequiredArgsConstructor
public class KafkaConsumerApplication {
    private volatile boolean keepConsuming = true;
    private final Consumer<String, String> consumer;
    private final ConsumerRecordsHandler<String, String> recordsHandler;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException(
                    "This program takes one argument: the path to an environment configuration file.");
        }
        Properties props = loadProperties(args[0]);
    }

    public void runConsume(Properties props) {
        try {
            consumer.subscribe(Collections.singletonList(props.getProperty("input.topic.name")));
            while (keepConsuming) {
                final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
                recordsHandler.process(consumerRecords);
            }
        } finally {
            consumer.close();
        }
    }

    private static Properties loadProperties(String fileName) throws IOException {
        Properties props = new Properties();
        try (FileInputStream inputStream = new FileInputStream(fileName)) {
            props.load(inputStream);
        }
        return props;
    }
}
