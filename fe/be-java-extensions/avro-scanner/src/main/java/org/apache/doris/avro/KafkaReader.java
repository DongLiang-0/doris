package org.apache.doris.avro;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Queue;

public class KafkaReader extends AvroReader {
    private static final Logger LOG = LogManager.getLogger(KafkaReader.class);
    private final Properties props = new Properties();
    private final Map<String, String> requiredParams;
    private final String kafkaSchemaPath;
    private KafkaConsumer<String, GenericRecord> consumer;
    private Schema schema;
    private final String topic;
    private long startOffset;
    private long maxRows;
    private long readRows;
    private int partition;
    private final Queue<ConsumerRecord<String, GenericRecord>> recordsQueue;
    private final int pollTimeout = 3000;

    public KafkaReader(Map<String, String> requiredParams) {
        this.requiredParams = requiredParams;
        this.kafkaSchemaPath = requiredParams.get(AvroProperties.KAFKA_SCHEMA_PATH);
        this.topic = requiredParams.get(AvroProperties.KAFKA_TOPIC);
        this.recordsQueue = new LinkedList<>();
    }

    @Override
    public void open(AvroFileContext avroFileContext, boolean tableSchema) throws IOException {
        initKafkaProps();
        if (tableSchema) {
            SchemaRegistryClient registryClient = new CachedSchemaRegistryClient(
                    props.getProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG),
                    AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT);
            try {
                Collection<String> topicSubjects = registryClient.getAllSubjectsByPrefix(topic);
                for (String topicSubject : topicSubjects) {
                    if (topicSubject.endsWith("-value")) {
                        List<Integer> allVersions = registryClient.getAllVersions(topicSubject);
                        if (allVersions.size() > 1) {
                            String errMsg = String.format(
                                    "kafka avro scanner does not support multiple avro schemas for one topic. "
                                            + "topic=%s, allVersions=%s", topic, allVersions.toString());
                            LOG.error(errMsg);
                            throw new IOException(errMsg);
                        }
                        SchemaMetadata latestSchemaMetadata = registryClient.getLatestSchemaMetadata(topicSubject);
                        String schemaType = latestSchemaMetadata.getSchemaType();
                        if (!schemaType.equalsIgnoreCase("AVRO")) {
                            String errMsg = String.format(
                                    "The schema of the current kafka topic is not an avro structure. topic=%s, schemaType=%s",
                                    topic, schemaType);
                            LOG.error(errMsg);
                            throw new IOException(errMsg);
                        }
                        String schemaStr = latestSchemaMetadata.getSchema();
                        this.schema = new Parser().parse(schemaStr);
                    }
                }
            } catch (RestClientException e) {
                LOG.error("Failed to get kafka avro schema.", e);
                throw new RuntimeException("Failed to get kafka avro schema.", e);
            }
            return;
        }
        pollRecords();
    }

    private void initKafkaProps() {
        if (requiredParams.containsKey(AvroProperties.SPLIT_SIZE)) {
            this.partition = Integer.parseInt(requiredParams.get(AvroProperties.SPLIT_SIZE));
        }
        if (requiredParams.containsKey(AvroProperties.SPLIT_START_OFFSET)) {
            this.startOffset = Long.parseLong(requiredParams.get(AvroProperties.SPLIT_START_OFFSET));
        }
        if (requiredParams.containsKey(AvroProperties.SPLIT_FILE_SIZE)) {
            this.maxRows = Long.parseLong(requiredParams.get(AvroProperties.SPLIT_FILE_SIZE));
        }
        props.put(AvroProperties.KAFKA_BOOTSTRAP_SERVERS,
                requiredParams.get(AvroProperties.KAFKA_BROKER_LIST));
        props.put(AvroProperties.KAFKA_GROUP_ID, requiredParams.get(AvroProperties.KAFKA_GROUP_ID));
        props.put(AvroProperties.KAFKA_SCHEMA_REGISTRY_URL,
                requiredParams.get(AvroProperties.KAFKA_SCHEMA_REGISTRY_URL));
        props.put(AvroProperties.KAFKA_KEY_DESERIALIZER, AvroProperties.KAFKA_STRING_DESERIALIZER);
        props.put(AvroProperties.KAFKA_VALUE_DESERIALIZER, AvroProperties.KAFKA_AVRO_DESERIALIZER);
        props.put(AvroProperties.KAFKA_AUTO_COMMIT_ENABLE, "true");

        // For kafka certification
        if (requiredParams.containsKey(AvroProperties.KAFKA_SECURITY_PROTOCOL)) {
            props.put(AvroProperties.KAFKA_SECURITY_PROTOCOL,
                    requiredParams.get(AvroProperties.KAFKA_SECURITY_PROTOCOL));
        }
        if (requiredParams.containsKey(AvroProperties.KAFKA_SAS1_MECHANISM)) {
            props.put(AvroProperties.KAFKA_SAS1_MECHANISM, requiredParams.get(AvroProperties.KAFKA_SAS1_MECHANISM));
        }
        if (requiredParams.containsKey(AvroProperties.KAFKA_SAS1_KERBEROS_SERVICE_NAME)) {
            props.put(AvroProperties.KAFKA_SAS1_KERBEROS_SERVICE_NAME,
                    requiredParams.get(AvroProperties.KAFKA_SAS1_KERBEROS_SERVICE_NAME));
        }
        if (requiredParams.containsKey(AvroProperties.KAFKA_SAS1_JAAS_CONFIG)) {
            props.put(AvroProperties.KAFKA_SAS1_JAAS_CONFIG, requiredParams.get(AvroProperties.KAFKA_SAS1_JAAS_CONFIG));
        }
    }

    private void pollRecords() {
        this.consumer = new KafkaConsumer<>(props);
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.assign(Collections.singletonList(topicPartition));
        // long position = consumer.position(topicPartition);
        // if (position < startOffset) {
        //     LOG.warn("The position of Kafka's topic=" + topic + " and partition=" + partition
        //             + " is less than the starting offset. Partition position=" + position + "");
        //     // startOffset = position + 1;
        // }
        consumer.seek(topicPartition, startOffset);
        long tmpPollTime = System.currentTimeMillis();
        while (readRows < maxRows) {
            // When the data set in the partition from startOffset to the latest offset does not reach the data volume of maxRows.
            // After the reading timeout period is reached, the consumer program is actively released.
            if (System.currentTimeMillis() - tmpPollTime > pollTimeout) {
                break;
            }
            ConsumerRecords<String, GenericRecord> records = consumer.poll(200);
            for (ConsumerRecord<String, GenericRecord> record : records) {
                if (readRows < maxRows) {
                    recordsQueue.offer(record);
                }
                tmpPollTime = System.currentTimeMillis();
                readRows++;
            }
        }
        consumer.close();
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public boolean hasNext(AvroWrapper<Pair<Integer, Long>> inputPair, NullWritable ignore) throws IOException {
        return CollectionUtils.isNotEmpty(recordsQueue);
    }

    @Override
    public Object getNext() {
        ConsumerRecord<String, GenericRecord> record = recordsQueue.poll();
        GenericRecord recordValue = Objects.requireNonNull(record).value();
        int partition = record.partition();
        String key = record.key();
        long offset = record.offset();
        LOG.info("partition=" + partition + " offset =" + offset + " key=" + key + " value="
                + recordValue.toString());
        return recordValue;
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(consumer)) {
            consumer.close();
        }
    }
}
