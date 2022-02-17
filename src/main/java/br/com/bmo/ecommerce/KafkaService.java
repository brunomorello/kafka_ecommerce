package br.com.bmo.ecommerce;

import br.com.bmo.ecommerce.functions.ConsumerFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

public class KafkaService implements Closeable {
    private final KafkaConsumer<String, String> consumer;
    private final ConsumerFunction parse;

    public KafkaService(String groupdId, String topic, ConsumerFunction parse) {
        this(groupdId, parse);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(String groupId, Pattern topics, ConsumerFunction parse) {
        this(groupId, parse);
        consumer.subscribe(topics);
    }

    private KafkaService(String groupdId, ConsumerFunction parse) {
        this.consumer = new KafkaConsumer<>(getProperties(groupdId));
        this.parse = parse;
    }

    public void run() {
        while(true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if(!records.isEmpty()) {
                System.out.println("# " + records.count() + " Records found.");
            }
            for (var record : records) {
                parse.consume(record);
            }
        }
    }

    private static Properties getProperties(String groupdId) {
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupdId);
        prop.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        prop.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return prop;
    }

    @Override
    public void close()  {
        consumer.close();
    }
}
