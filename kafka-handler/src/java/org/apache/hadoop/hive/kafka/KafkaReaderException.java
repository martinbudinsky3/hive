package org.apache.hadoop.hive.kafka;

public class KafkaReaderException extends Exception {
    public KafkaReaderException() {
        super();
    }

    public KafkaReaderException(String message) {
        super(message);
    }
}
