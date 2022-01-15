package ecommerce.consumers;

import ecommerce.GlobalConstants;
import ecommerce.producers.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import static ecommerce.GlobalConstants.sleep;

public class FraudDetectorService {

    public static void main(String[] args)  {
        var fraudDetectorService = new FraudDetectorService();
        try(var kafkaService =
                new KafkaService<>(FraudDetectorService.class.getName(),
                        GlobalConstants.ECOMMERCE_NEW_ORDER_TOPIC, fraudDetectorService::consume
                        , Order.class);) {
            kafkaService.run();
        }

    }

    private void consume(ConsumerRecord<String, Order> record) {
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
