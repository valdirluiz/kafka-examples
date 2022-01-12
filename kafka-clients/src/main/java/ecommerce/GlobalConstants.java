package ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class GlobalConstants {

    public static final String ECOMMERCE_NEW_ORDER_TOPIC = "ecommerce_new_order";
    public static final String ECOMMERCE_SEND_EMAIL_TOPIC = "ecommerce_send_email";
    public static final String BOOTSTRAP_SERVERS_CONFIG_VALUE = "127.0.0.1:9092";

    public static Properties consumerProperties(String name) {
        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, GlobalConstants.BOOTSTRAP_SERVERS_CONFIG_VALUE);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, name);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return properties;
    }
}
