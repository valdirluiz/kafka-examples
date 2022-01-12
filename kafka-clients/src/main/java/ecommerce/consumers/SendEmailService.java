package ecommerce.consumers;

import ecommerce.GlobalConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;

import static ecommerce.GlobalConstants.consumerProperties;
import static java.util.Collections.singletonList;

public class SendEmailService {

    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(consumerProperties(SendEmailService.class.getName()));
        consumer.subscribe(singletonList(GlobalConstants.ECOMMERCE_SEND_EMAIL_TOPIC));
        while (true){
            poll(consumer);
         }
    }

    private static void poll(KafkaConsumer<String, String> consumer) {
        var records = consumer.poll(Duration.ofMillis(100));
        for(var record : records){
            System.out.println("---------------------------------------------");
            System.out.println("Sending Email. ");
            System.out.println("Order key: " + record.key());
            System.out.println("Order payload: " + record.value());
            System.out.println("Order partition: " + record.partition());
            System.out.println("Order offset: " + record.offset());
            System.out.println("Email sent...");
            sleep(1000);
            System.out.println("---------------------------------------------");

        }
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
