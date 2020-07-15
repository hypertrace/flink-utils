package org.hypertrace.core.flinkutils.avro;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegistryBasedAvroSerde<T extends SpecificRecord> implements
    DeserializationSchema<T>,
    SerializationSchema<T>, Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(RegistryBasedAvroSerde.class);
  protected final String topicName;
  protected final Map<String, String> serdeConfig;
  protected final Class<T> clazz;
  protected transient KafkaAvroSerializer serializer;
  protected transient KafkaAvroDeserializer deserializer;
  protected transient Schema readerSchema;


  public RegistryBasedAvroSerde(String topicName, Class<T> clazz, Map<String, String> serdeConfig) {
    this.topicName = topicName;
    this.clazz = clazz;
    this.serdeConfig = serdeConfig;
    initTransients();
  }

  @Override
  public T deserialize(byte[] message) {
    try {
      T record;
      if (readerSchema != null) {
        record = (T) deserializer.deserialize(topicName, message, readerSchema);
      } else {
        record = (T) deserializer.deserialize(topicName, message);
      }
      return record;
    } catch (Exception e) {
      LOGGER.error("Failed to deserialize message bytes of " + clazz.getName(), e);
    }
    return null;
  }

  @Override
  public boolean isEndOfStream(T nextElement) {
    return false;
  }

  @Override
  public TypeInformation<T> getProducedType() {
    return TypeInformation.of(this.clazz);
  }

  /**
   * @param element to serialize
   * @return serialized bytes
   * @throws org.apache.kafka.common.errors.SerializationException
   */
  @Override
  public byte[] serialize(T element) {
    return serializer.serialize(topicName, element);
  }

  private void readObject(ObjectInputStream inputStream)
      throws IOException, ClassNotFoundException {
    inputStream.defaultReadObject();
    initTransients();
  }

  protected void initTransients() {
    this.serializer = new KafkaAvroSerializer();
    // Required. this triggers the initialization of underlying schema registry client
    this.serializer.configure(serdeConfig, false);

    this.deserializer = new KafkaAvroDeserializer();
    // Required. this triggers the initialization of underlying schema registry client
    this.deserializer.configure(serdeConfig, false);
    initReaderSchema();
  }

  /**
   * Extract the reader schema from the pay load class object
   */
  private void initReaderSchema() {
    try {
      if (readerSchema == null) {
        Method m_getClassSchema = clazz.getMethod("getClassSchema", (Class<?>[]) null);
        if (m_getClassSchema != null) {
          readerSchema = (Schema) m_getClassSchema.invoke(null, (Object[]) null);
        }
      }
    } catch (Exception e) {
      // ideally this shouldn't happen
      LOGGER.error("Failed to extract reader schema for class = [" + clazz.getName() + "]", e);
    }
  }
}

