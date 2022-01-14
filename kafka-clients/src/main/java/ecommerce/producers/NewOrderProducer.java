package ecommerce.producers;

import ecommerce.GlobalConstants;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(var dispatcher = new KafkaDispatcher();) {
            for (int i = 0; i <= 10; i++) {
                sendOrder(dispatcher);
                sendEmail(dispatcher);
            }
        }

    }

    private static void sendEmail(KafkaDispatcher dispatcher) throws ExecutionException, InterruptedException {
        var body = "valdir@gmail.com; Thank you for your order! We are processing your order!";
        dispatcher.send(GlobalConstants.ECOMMERCE_SEND_EMAIL_TOPIC, UUID.randomUUID().toString(), body);
    }

    private static void sendOrder(KafkaDispatcher dispatcher) throws ExecutionException, InterruptedException {
        var value = "123,678,987654";
        var key = UUID.randomUUID().toString();
        dispatcher.send(GlobalConstants.ECOMMERCE_NEW_ORDER_TOPIC, key, value);
    }


}
