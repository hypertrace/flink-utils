package org.hypertrace.core.flinkutils.avro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroGenericRecordSchema implements
    DeserializationSchema<GenericRecord>,
    SerializationSchema<GenericRecord>, Serializable {

  private static Logger LOGGER = LoggerFactory.getLogger(AvroGenericRecordSchema.class);
  private String avroSchema;
  private transient Schema schema;
  private transient DatumWriter<GenericRecord> writer;
  private transient GenericDatumReader<GenericRecord> reader;


  public AvroGenericRecordSchema(String avroSchema) {
    this.avroSchema = avroSchema;
    initTransients();
  }

  private void initTransients() {
    this.schema = new Schema.Parser().parse(avroSchema);
    this.writer = new GenericDatumWriter<>(this.schema);
    this.reader = new GenericDatumReader(this.schema);
  }

  @Override
  public GenericRecord deserialize(byte[] message) {
    try {
      GenericRecord record = reader.read(null, DecoderFactory.get()
          .directBinaryDecoder(new ByteArrayInputStream(message), null));
      // LOGGER.info("Successfully deserialize message to Avro Record: {}.", record);
      return record;
    } catch (Exception e) {
      LOGGER.error("Failed to deserialize message bytes.", e);
    }
    return null;
  }

  @Override
  public boolean isEndOfStream(GenericRecord nextElement) {
    return false;
  }

  @Override
  public TypeInformation<GenericRecord> getProducedType() {
    return TypeInformation.of(GenericRecord.class);
  }

  @Override
  public byte[] serialize(GenericRecord element) {
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
}

