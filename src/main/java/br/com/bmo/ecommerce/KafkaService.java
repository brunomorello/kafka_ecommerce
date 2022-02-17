package br.com.bmo.ecommerce;

import br.com.bmo.ecommerce.functions.ConsumerFunction;
import br.com.bmo.ecommerce.serializer.GsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {
    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFunction parse;

    public KafkaService(String groupdId, String topic, ConsumerFunction parse, Class<T> type, Map<String, String> properties) {
        this(groupdId, parse, type, properties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(String groupId, Pattern topics, ConsumerFunction parse, Class<T> type, Map<String, String> properties) {
        this(groupId, parse, type, properties);
        consumer.subscribe(topics);
    }

    private KafkaService(String groupdId, ConsumerFunction parse, Class<T> type, Map<String, String> properties) {
        this.consumer = new KafkaConsumer<>(getProperties(type, groupdId, properties));
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

    private Properties getProperties(Class<T> type, String groupdId, Map<String, String> overrideProperties) {
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupdId);
        prop.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        prop.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        prop.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
        prop.putAll(overrideProperties);
        return prop;
    }

    @Override
    public void close()  {
        consumer.close();
    }
}
