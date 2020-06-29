package org.hypertrace.core.flinkutils.utils;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.hypertrace.core.flinkutils.avro.AvroPayloadSchema;
import org.hypertrace.core.flinkutils.avro.RegistryBasedAvroSerde;

public class FlinkUtils {

  public static FlinkKafkaConsumer<?> createKafkaConsumer(String topic,
                                                          DeserializationSchema schema, Properties props) {
    return new FlinkKafkaConsumer<>(topic, schema, props);
  }

  /**
   * Use other overloaded methods where possible. This method will be removed in future versions.
   *
   * @see RegistryBasedAvroSerde
   */
  @Deprecated
  public static FlinkKafkaProducer getFlinkKafkaProducer(String topic, String schemaClass,
                                                         Properties kafkaProducerConfig, boolean logFailuresOnly) {
    return getFlinkKafkaProducer(topic, new AvroPayloadSchema(schemaClass), kafkaProducerConfig,
        logFailuresOnly);
  }

  public static FlinkKafkaProducer getFlinkKafkaProducer(String topic, SerializationSchema schema,
                                                         Properties kafkaProducerConfig, boolean logFailuresOnly) {
    return getFlinkKafkaProducer(topic, schema, new FlinkFixedPartitioner(), kafkaProducerConfig,
        logFailuresOnly);
  }

  public static FlinkKafkaProducer getFlinkKafkaProducer(String topic, SerializationSchema schema,
                                                         FlinkKafkaPartitioner partitioner, Properties kafkaProducerConfig, boolean logFailuresOnly) {
    final FlinkKafkaProducer producer = new FlinkKafkaProducer(
        topic,
        new KeyedSerializationSchemaWrapper<>(schema),
        kafkaProducerConfig,
        Optional.ofNullable(partitioner)
    );
    producer.setLogFailuresOnly(logFailuresOnly);
    return producer;
  }

  /**
   * Today all flink jobs are run in standalone mode and run in LocalEnvironment. This method is a
   * simple utility to pass the configuration and create a {@link org.apache.flink.streaming.api.environment.LocalStreamEnvironment}
   * <p>
   * Refer to: https://stackoverflow.com/questions/58922246/apache-flink-local-setup-practical-differences-between-standalone-jar-vs-start
   * on why this is the way to pass any custom config
   * <p>
   * This method needs to change if the flink job were to run in a clustered mode
   *
   * @param properties
   * @return runtime env in which the flink job runs
   */
  public static StreamExecutionEnvironment getExecutionEnvironment(Map<String, String> properties) {
    Configuration configuration = new Configuration();
    properties.forEach(configuration::setString);
    return StreamExecutionEnvironment
        .createLocalEnvironment(Runtime.getRuntime().availableProcessors(), configuration);
  }
}
