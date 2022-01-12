package ecommerce.consumers;

import ecommerce.GlobalConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;

import static ecommerce.GlobalConstants.consumerProperties;
import static java.util.Collections.singletonList;

public class FraudDetectorService {

    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(consumerProperties(FraudDetectorService.class.getName()));
        consumer.subscribe(singletonList(GlobalConstants.ECOMMERCE_NEW_ORDER_TOPIC));
        while (true){
            poll(consumer);
         }
    }

    private static void poll(KafkaConsumer<String, String> consumer) {
        var records = consumer.poll(Duration.ofMillis(100));
        for(var record : records){
            System.out.println("---------------------------------------------");
            System.out.println("Processing new order, checking for fraud. ");
            System.out.println("Order key: " + record.key());
            System.out.println("Order payload: " + record.value());
            System.out.println("Order partition: " + record.partition());
            System.out.println("Order offset: " + record.offset());
            System.out.println("Order processed...");
            sleep(5000);
            System.out.println("---------------------------------------------");

        }
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, GlobalConstants.BOOTSTRAP_SERVERS_CONFIG_VALUE);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getName());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return properties;
    }

    private static void sleep(long timeToSleep) {
        try {
            Thread.sleep(timeToSleep);
        } catch (InterruptedException e) {
            // ignoring
            e.printStackTrace();
        }
    }

}
