package io.muayed.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

//@Slf4j
public class CustomSerializer implements Deserializer<Performance> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Performance deserialize(String s, byte[] bytes) {
        try {
            if (bytes == null){
                System.out.println("Null received at serializing");
                return null;
            }
            System.out.println("Deserializing...");
            return objectMapper.readValue(bytes, Performance.class);
        } catch (Exception e) {
            throw new SerializationException("Error when serializing MessageDto to byte[]");
        }
    }


    @Override
    public void close() {
    }
}