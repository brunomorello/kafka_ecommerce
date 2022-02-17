package br.com.bmo.ecommerce;

import br.com.bmo.ecommerce.model.EmailNotification;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class EmailService {
    public static void main(String[] args) {
        var emailService = new EmailService();
        try (var service = new KafkaService<EmailNotification>(
                EmailService.class.getSimpleName(),
                "STORE_SEND_EMAIL",
                emailService::parse,
                EmailNotification.class,
                Map.of())) {
            service.run();

        }
    }

    private void parse(ConsumerRecord<String, EmailNotification> record) {
        System.out.println("--------------------------------------");
        System.out.println("Sending e-mail");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Email sent");
    }
}
