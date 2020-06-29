package org.hypertrace.core.flinkutils.avro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Use registry based schema with flink kafka producer/consumer.
 *
 * @see RegistryBasedAvroSerde
 */
public class AvroPayloadSchema<T extends SpecificRecord> implements
    DeserializationSchema<T>,
    SerializationSchema<T>, Serializable {

  private static Logger LOGGER = LoggerFactory.getLogger(AvroPayloadSchema.class);
  private String className;
  private transient Class<T> clazz;
  private transient ReflectDatumReader<T> reader;
  private transient DatumWriter<T> writer;

  public AvroPayloadSchema(String className) {
    this.className = className;
    initTransients();
  }

  @Override
  public T deserialize(byte[] message) {
    try {
      T record = reader.read(null, DecoderFactory.get()
          .directBinaryDecoder(new ByteArrayInputStream(message), null));
      // LOGGER.info("Successfully deserialize message to Avro Record: {}.", record);
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

  @Override
  public byte[] serialize(T element) {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(os, null);
    try {
      writer.write(element, encoder);
      encoder.flush();
      return os.toByteArray();
    } catch (IOException e) {
      return new byte[0];
    }
  }


  private void readObject(ObjectInputStream inputStream)
      throws IOException, ClassNotFoundException {
    inputStream.defaultReadObject();
    initTransients();
  }

  private void initTransients() {

    try {
      this.clazz = (Class<T>) Thread.currentThread().getContextClassLoader()
          .loadClass(this.className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    this.reader = new ReflectDatumReader(clazz);
    this.writer = new SpecificDatumWriter<>(clazz);
  }
}

