package io.confluent.developer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class FileWritingRecordsHandler implements ConsumerRecordsHandler<String, String> {
    private final Path path;

    @Override
    public void process(ConsumerRecords<String, String> consumerRecords) {
        final List<String> valueList = new ArrayList<>();
        consumerRecords.forEach(record -> valueList.add(record.value()));
        if (!valueList.isEmpty()) {
            try {
                Files.write(path, valueList, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
