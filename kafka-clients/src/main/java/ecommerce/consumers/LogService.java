package ecommerce.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.regex.Pattern;


public class LogService {

    public static void main(String[] args) {
        var logService = new LogService();
        try(var kafkaService =
                    new KafkaService<>(FraudDetectorService.class.getName(),
                            Pattern.compile("ecommerce.*"), logService::consume, String.class);) {
            kafkaService.run();
        }
    }

    private void consume(ConsumerRecord<String, String> record) {
        System.out.println("---------------------------------------------");
        System.out.println("LOG: " + record.topic());
        System.out.println("Order key: " + record.key());
        System.out.println("Order payload: " + record.value());
        System.out.println("Order partition: " + record.partition());
        System.out.println("Order offset: " + record.offset());
        System.out.println("---------------------------------------------");
    }







}
