package ecommerce.producers;

import ecommerce.GlobalConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String, String>(properties());
        var value = "123,678,987654";
        var key = "123";
        var record = new ProducerRecord<>(GlobalConstants.TOPIC_NAME, key, value);
       producer.send(record, ((data, ex) -> {
            if(ex!=null){
                ex.printStackTrace();
            } else {
                System.out.printf("::: Message has been sent! Result: partition: %d /offset: %d /timestamp: %d%n",
                        data.partition(), data.offset(), data.timestamp());
            }
       })).get();
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, GlobalConstants.BOOTSTRAP_SERVERS_CONFIG_VALUE);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

}
