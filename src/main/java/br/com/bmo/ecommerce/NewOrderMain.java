package br.com.bmo.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String, String>(getProperties());
        var value = "3312,3212,102";
        var record = new ProducerRecord<>("STORE_NEW_ORDER", value, value);
        Callback callback = (metadata, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
                return;
            }

            System.out.println("success!\n\n" + metadata.topic() + " > partition: " + metadata.partition() + " offset: " + metadata.offset() + " timestamp: " + metadata.timestamp());
        };
        producer.send(record, callback).get();

        String emailContent = "Thanks for buying with us! Your Order is being processed";
        ProducerRecord<String, String> emailRecord = new ProducerRecord<>("STORE_SEND_EMAIL", emailContent, emailContent);
        producer.send(emailRecord, callback).get();
    }

    private static Properties getProperties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
