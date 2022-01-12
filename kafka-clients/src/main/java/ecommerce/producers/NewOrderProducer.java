package ecommerce.producers;

import ecommerce.GlobalConstants;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        for(int i =0; i<=100;i++){
            var producer = new KafkaProducer<String, String>(properties());
            var value = "123,678,987654";
            var key = UUID.randomUUID().toString();
            var orderRecord = new ProducerRecord<>(GlobalConstants.ECOMMERCE_NEW_ORDER_TOPIC, key, value);
            producer.send(orderRecord, callback()).get();

            var body = "valdir@gmail.com; Thank you for your order! We are processing your order!";
            var emailRecord = new ProducerRecord<>(GlobalConstants.ECOMMERCE_SEND_EMAIL_TOPIC, UUID.randomUUID().toString(), body);
            producer.send(emailRecord, callback()).get();
        }

    }

    private static Callback callback() {
        return (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
            } else {
                System.out.printf("::: Message has been sent! Result: partition: %d /offset: %d /timestamp: %d%n",
                        data.partition(), data.offset(), data.timestamp());
            }
        };
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, GlobalConstants.BOOTSTRAP_SERVERS_CONFIG_VALUE);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

}
