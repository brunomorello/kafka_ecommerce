package br.com.bmo.ecommerce;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (KafkaDispatcher dispatcher = new KafkaDispatcher()) {
            for (int i = 0; i < 10; i++) {
                var key = UUID.randomUUID().toString();
                var value = "3312,3212,102";
                dispatcher.send("STORE_NEW_ORDER", key, value);

                var emailContent = "Thanks for buying with us! Your Order is being processed";
                dispatcher.send("STORE_SEND_EMAIL", key, emailContent);
            }
        }
    }
}
