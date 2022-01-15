package ecommerce.producers;

import ecommerce.GlobalConstants;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(var orderDispatcher = new KafkaDispatcher<Order>();
            var emailDispatcher = new KafkaDispatcher<String>();) {
            for (int i = 0; i <= 10; i++) {
                sendOrder(orderDispatcher);
                sendEmail(emailDispatcher);
            }
        }

    }

    private static void sendEmail(KafkaDispatcher dispatcher) throws ExecutionException, InterruptedException {
        var body = "valdir@gmail.com; Thank you for your order! We are processing your order!";
        dispatcher.send(GlobalConstants.ECOMMERCE_SEND_EMAIL_TOPIC, UUID.randomUUID().toString(), body);
    }

    private static void sendOrder(KafkaDispatcher dispatcher) throws ExecutionException, InterruptedException {


        var userId = UUID.randomUUID().toString();
        var orderId = UUID.randomUUID().toString();
        var amount = new BigDecimal(Math.random() * 5000 + 1);

        var order = new Order(userId, orderId, amount);

        dispatcher.send(GlobalConstants.ECOMMERCE_NEW_ORDER_TOPIC, orderId, order);
    }


}
