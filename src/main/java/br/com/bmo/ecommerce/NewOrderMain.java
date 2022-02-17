package br.com.bmo.ecommerce;

import br.com.bmo.ecommerce.model.EmailNotification;
import br.com.bmo.ecommerce.model.Order;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (KafkaDispatcher orderDispatcher = new KafkaDispatcher<Order>()) {
            try (KafkaDispatcher emailDispatcher = new KafkaDispatcher<EmailNotification>()) {
                for (int i = 0; i < 10; i++) {

                    var userId = UUID.randomUUID().toString();
                    var orderId = UUID.randomUUID().toString();
                    var amount = new BigDecimal(Math.random() * 5000 + 1);

                    var order = new Order(userId, orderId, amount);
                    orderDispatcher.send("STORE_NEW_ORDER", userId, order);

                    var emailContent = "Thanks for buying with us! Your Order is being processed";
                    var email = new EmailNotification(null, emailContent);
                    emailDispatcher.send("STORE_SEND_EMAIL", userId, email);
                }
            }
        }
    }
}
