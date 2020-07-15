package org.hypertrace.core.flinkutils.avro;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.hypertrace.core.common.flinkutils.test.api.TestView;
import org.junit.jupiter.api.Test;

public class RegistryBasedAvroSerdeTest {

  private String topicName = "input";

  @Test
  public void whenReaderSchemaIsNull_thenExpectIttoBeNotPassedInDeserialize() {
    CustomRegistryBasedAvroSerde<TestView> underTest = new CustomRegistryBasedAvroSerde<>(topicName,
        TestView.class,
        Collections.emptyMap(), false);
    byte[] data = underTest.serialize(createTestView());
    underTest.deserialize(data);
    assertFalse(underTest.invokedWithReaderSchema);
  }

  @Test
  public void whenReaderSchemaExists_thenExpectIttoBePassedInDeserialize() {
    CustomRegistryBasedAvroSerde<TestView> underTest = new CustomRegistryBasedAvroSerde<>(topicName,
        TestView.class,
        Collections.emptyMap(), true);
    byte[] data = underTest.serialize(createTestView());
    underTest.deserialize(data);
    assertTrue(underTest.invokedWithReaderSchema);
  }

  private TestView createTestView() {
    return TestView.newBuilder()
        .setCreationTimeMillis(20L)
        .setFriends(List.of("john", "sam"))
        .setIdSha(ByteBuffer.wrap("test-sha".getBytes()))
        .setName("alex")
        .setTimeTakenMillis(30L)
        .setProperties(Map.of("loc", "SF"))
        .build();

  }

  // makes it feasible to test with mock registry client
  static class CustomRegistryBasedAvroSerde<T extends SpecificRecord> extends
      RegistryBasedAvroSerde<T> {

    boolean invokedWithReaderSchema;

    public CustomRegistryBasedAvroSerde(String topicName, Class clazz,
        Map<String, String> serdeConfig, boolean shouldInitReaderSchema) {
      super(topicName, clazz, serdeConfig);
      if (shouldInitReaderSchema) {
        this.readerSchema = TestView.getClassSchema();
      }
    }

    @Override
    protected void initTransients() {
      MockSchemaRegistryClient client = new MockSchemaRegistryClient();
      try {
        client.register(topicName + "-value", TestView.getClassSchema());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      this.serializer = new KafkaAvroSerializer(client);
      this.deserializer = new KafkaAvroDeserializer(client) {
        @Override
        public Object deserialize(String s, byte[] bytes) {
          return super.deserialize(s, bytes);
        }

        @Override
        public Object deserialize(String s, byte[] bytes, Schema readerSchema) {
          invokedWithReaderSchema = true;
          return super.deserialize(s, bytes);
        }
      };
    }
  }
}
