package ecommerce.consumers;
import ecommerce.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService {

    public static void main(String[] args)  {
        var fraudDetectorService = new FraudDetectorService();
        try(var kafkaService =
                new KafkaService<>(FraudDetectorService.class.getName(),
                        "ecommerce_new_order", fraudDetectorService::consume
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
        System.out.println("---------------------------------------------");
    }

}
