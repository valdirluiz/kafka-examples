package ecommerce.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class SendEmailService {

    public static void main(String[] args) {
        var sendEmailService = new SendEmailService();
        try(var kafkaService =
                new KafkaService<>(SendEmailService.class.getName(),
                        "ecommerce_send_email", sendEmailService::consume, String.class);) {
            kafkaService.run();
        }
    }

    private void consume(ConsumerRecord<String, String> record) {
        System.out.println("---------------------------------------------");
        System.out.println("Sending Email. ");
        System.out.println("Order key: " + record.key());
        System.out.println("Order payload: " + record.value());
        System.out.println("Order partition: " + record.partition());
        System.out.println("Order offset: " + record.offset());
        System.out.println("Email sent...");
        System.out.println("---------------------------------------------");

    }

}
