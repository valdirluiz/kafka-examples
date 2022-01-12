package ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class GlobalConstants {

    public static final String ECOMMERCE_NEW_ORDER_TOPIC = "ecommerce_new_order";
    public static final String ECOMMERCE_SEND_EMAIL_TOPIC = "ecommerce_send_email";
    public static final String BOOTSTRAP_SERVERS_CONFIG_VALUE = "127.0.0.1:9092";

    public static void sleep(long timeToSleep) {
        try {
            Thread.sleep(timeToSleep);
        } catch (InterruptedException e) {
            // ignoring
            e.printStackTrace();
        }
    }
}
