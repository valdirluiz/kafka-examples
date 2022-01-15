package ecommerce.consumers;

import ecommerce.producers.Order;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {

    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFunction parse;
    private final Class<T> type;

    KafkaService(String groupId, String topic, ConsumerFunction parse, Class<T> type) {
        this(parse, groupId, type);
        consumer.subscribe(Collections.singletonList(topic));
    }

    KafkaService(String groupId, Pattern topic, ConsumerFunction parse,  Class<T> type) {
        this(parse, groupId, type);
        consumer.subscribe(topic);
    }

    private KafkaService(ConsumerFunction parse, String groupId,  Class<T> type) {
        this.type = type;
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(properties(groupId));
    }



    void run() {
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                for (var record : records) {
                    parse.consume(record);
                }
            }
        }
    }

    private Properties properties(String groupId) {
        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.put(GsonDeserializer.TYPE_CONFIG, type.getName());
        return properties;
    }

    @Override
    public void close()  {
        this.consumer.close();
    }
}
