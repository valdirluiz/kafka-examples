package ecommerce.consumers;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.regex.Pattern;

import static ecommerce.GlobalConstants.consumerProperties;

public class LogService {

    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(consumerProperties(LogService.class.getName()));
        consumer.subscribe(Pattern.compile("ecommerce.*"));

        while(true) poll(consumer);
    }

    private static void poll(KafkaConsumer<String, String> consumer) {
        var records = consumer.poll(Duration.ofMillis(100));
        for(var record : records){
            System.out.println("---------------------------------------------");
            System.out.println("LOG: " + record.topic());
            System.out.println("Order key: " + record.key());
            System.out.println("Order payload: " + record.value());
            System.out.println("Order partition: " + record.partition());
            System.out.println("Order offset: " + record.offset());
            System.out.println("---------------------------------------------");

        }
    }



}
