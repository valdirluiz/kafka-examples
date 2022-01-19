package ecommerce.consumers;
import ecommerce.Order;
import ecommerce.producers.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    public static void main(String[] args)  {
        var fraudDetectorService = new FraudDetectorService();
        try(var kafkaService =
                new KafkaService<>(FraudDetectorService.class.getName(),
                        "ecommerce_new_order", fraudDetectorService::consume
                        , Order.class);) {
            kafkaService.run();
        }

    }

    private void consume(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException {
        System.out.println("---------------------------------------------");
        System.out.println("Processing new order, checking for fraud. ");
        System.out.println("Order key: " + record.key());
        System.out.println("Order payload: " + record.value());
        System.out.println("Order partition: " + record.partition());
        System.out.println("Order offset: " + record.offset());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Order order = record.value();
        if( isFraud(order)){
            System.out.println("Order is a fraud!");
            orderDispatcher.send("ecommerce_order_reject", order.getOrderId(), order);
        } else{
            System.out.println("Approved order...");
            orderDispatcher.send("ecommerce_order_approved", order.getOrderId(), order);

        }
        System.out.println("---------------------------------------------");
    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }

}
