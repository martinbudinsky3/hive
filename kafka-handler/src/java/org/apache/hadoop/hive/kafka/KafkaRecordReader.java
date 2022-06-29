/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.kafka;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Kafka Records Reader implementation.
 */
@SuppressWarnings("WeakerAccess") public class KafkaRecordReader extends RecordReader<NullWritable, KafkaWritable>
    implements org.apache.hadoop.mapred.RecordReader<NullWritable, KafkaWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaRecordReader.class);

  private KafkaConsumer<byte[], byte[]> consumer = null;
  private Configuration config = null;
  private KafkaWritable currentWritableValue;
  private Iterator<ConsumerRecord<byte[], byte[]>> recordsCursor = null;
  private SchemaRegistryClient schemaRegistryClient;
  private List<Integer> subjectIds = new ArrayList();

  private long totalNumberRecords = 0L;
  private long consumedRecords = 0L;
  private long readBytes = 0L;
  private volatile boolean started = false;
  private boolean isAvroTopic = false;

  @SuppressWarnings("WeakerAccess") public KafkaRecordReader() {
  }

  private void initConsumer() {
    if (consumer == null) {
      LOG.info("Initializing Kafka Consumer");
      final Properties properties = KafkaUtils.consumerProperties(config);
      String brokerString = properties.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
      Preconditions.checkNotNull(brokerString, "broker end point can not be null");
      LOG.info("Starting Consumer with Kafka broker string [{}]", brokerString);
      consumer = new KafkaConsumer<>(properties);
    }

    isAvroTopic = checkAvro();

    if(isAvroTopic) {
      fetchSubjectIds();
    }
  }

  private boolean checkAvro() {
    String schemaUrl = config.get(AvroSerdeUtils.AvroTableProperties.SCHEMA_URL.getPropName());

    return !(schemaUrl == null || "".equals(schemaUrl));
  }

  private void fetchSubjectIds() {
      String schemaRegistryUrl = AvroSerdeUtils.getBaseUrl(config);
      LOG.debug("Schema Url: {}", schemaRegistryUrl);
      schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 10);
      String subject = AvroSerdeUtils.getSubject(config);
      LOG.debug("Subject: {}", subject);
      Preconditions.checkNotNull(subject);
      try {
        List<Integer> versions = schemaRegistryClient.getAllVersions(subject);
        subjectIds = versions.stream()
                .map(version -> fetchSubjectIdByVersion(subject, version))
                .collect(Collectors.toList());

        LOG.debug("Versions found: {}", StringUtils.join(subjectIds, ','));

      } catch (IOException | RestClientException e) {
        throw new RuntimeException(e);
      }
  }

  private int fetchSubjectIdByVersion(String subject, int version) {
    try {
        SchemaMetadata schemaMetadata = schemaRegistryClient.getSchemaMetadata(subject, version);
        return schemaMetadata.getId();
    } catch (IOException | RestClientException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("WeakerAccess") public KafkaRecordReader(KafkaInputSplit inputSplit,
      Configuration jobConf) {
    initialize(inputSplit, jobConf);
  }

  private synchronized void initialize(KafkaInputSplit inputSplit, Configuration jobConf) {
    if (!started) {
      this.config = jobConf;
      long startOffset = inputSplit.getStartOffset();
      long endOffset = inputSplit.getEndOffset();
      TopicPartition topicPartition = new TopicPartition(inputSplit.getTopic(), inputSplit.getPartition());
      Preconditions.checkState(startOffset >= 0 && startOffset <= endOffset,
          "Start [%s] has to be positive and Less than or equal to End [%s]", startOffset, endOffset);
      totalNumberRecords += endOffset - startOffset;
      initConsumer();
      long
          pollTimeout =
          config.getLong(KafkaTableProperties.KAFKA_POLL_TIMEOUT.getName(), -1);
      LOG.debug("Consumer poll timeout [{}] ms", pollTimeout);
      this.recordsCursor =
          startOffset == endOffset ?
              new EmptyIterator() :
              new KafkaRecordIterator(consumer, topicPartition, startOffset, endOffset, pollTimeout);
      started = true;
    }
  }

  @Override public void initialize(org.apache.hadoop.mapreduce.InputSplit inputSplit, TaskAttemptContext context) {
    initialize((KafkaInputSplit) inputSplit, context.getConfiguration());
  }

  @Override public boolean next(NullWritable nullWritable, KafkaWritable bytesWritable) {
    ConsumerRecord<byte[], byte[]> record;
    if (started && (record = nextRecord()) != null) {
      bytesWritable.set(record);
      return true;
    }
    return false;
  }

  private ConsumerRecord<byte[], byte[]> nextRecord() {
    if(!recordsCursor.hasNext()) {
      return null;
    }

    ConsumerRecord<byte[], byte[]> record = recordsCursor.next();
    consumedRecords += 1;
    readBytes += record.serializedValueSize();
    if (record.value() == null) {
      return nextRecord();
    }

    if (isAvroTopic && !checkSubject(record)) {
      return nextRecord();
    }

    return record;
  }

  private boolean checkSubject(ConsumerRecord<byte[], byte[]> record) {
    int subjectId = ByteBuffer.wrap(Arrays.copyOfRange(record.value(), 1, 5)).getInt();
    LOG.debug("Subject Id from record: {}", subjectId);

    return subjectId == 521;
    // return subjectIds.contains(subjectId);
  }

  @Override public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override public KafkaWritable createValue() {
    return new KafkaWritable();
  }

  @Override public long getPos() {
    return -1;
  }

  @Override public boolean nextKeyValue() {
    currentWritableValue = new KafkaWritable();
    if (next(NullWritable.get(), currentWritableValue)) {
      return true;
    }
    currentWritableValue = null;
    return false;
  }

  @Override public NullWritable getCurrentKey() {
    return NullWritable.get();
  }

  @Override public KafkaWritable getCurrentValue() {
    return Preconditions.checkNotNull(currentWritableValue);
  }

  @Override public float getProgress() {
    if (consumedRecords == 0) {
      return 0f;
    }
    if (consumedRecords >= totalNumberRecords) {
      return 1f;
    }
    return consumedRecords * 1.0f / totalNumberRecords;
  }

  @Override public void close() {
    LOG.trace("total read bytes [{}]", readBytes);
    if (consumer != null) {
      consumer.wakeup();
      consumer.close();
    }
  }

  /**
   * Empty iterator for empty splits when startOffset == endOffset, this is added to avoid clumsy if condition.
   */
  static final class EmptyIterator implements Iterator<ConsumerRecord<byte[], byte[]>> {
    @Override public boolean hasNext() {
      return false;
    }

    @Override public ConsumerRecord<byte[], byte[]> next() {
      throw new IllegalStateException("this is an empty iterator");
    }
  }
}
