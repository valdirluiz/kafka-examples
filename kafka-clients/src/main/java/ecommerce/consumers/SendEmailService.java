package ecommerce.consumers;

import ecommerce.GlobalConstants;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import static ecommerce.GlobalConstants.sleep;

public class SendEmailService {

    public static void main(String[] args) {
        var sendEmailService = new SendEmailService();
        var kafkaService =
                new KafkaService(SendEmailService.class.getName(),
                        GlobalConstants.ECOMMERCE_SEND_EMAIL_TOPIC, sendEmailService::consume);
        kafkaService.run();
    }

    private void consume(ConsumerRecord<String, String> record) {
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
