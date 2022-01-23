package ecommerce.producers;

import ecommerce.Order;

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
        var body = "Thank you for your order! We are processing your order!";
        dispatcher.send("ecommerce_send_email", UUID.randomUUID().toString(), body);
    }

    private static void sendOrder(KafkaDispatcher dispatcher) throws ExecutionException, InterruptedException {

        var orderId = UUID.randomUUID().toString();
        var amount = new BigDecimal(Math.random() * 5000 + 1);

        var email = Math.random() + "@email.com";
        var order = new Order(orderId, amount, email);

        dispatcher.send("ecommerce_new_order", email, order);
    }


}
