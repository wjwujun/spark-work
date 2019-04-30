package kafka.demo;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


/*
* kafka生产者
* */
public class ProducerDemo {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("metadata.broker.list", "192.168.255.129:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        for (int i = 1; i <= 100; i++){
            producer.send(new KeyedMessage<>("kafka", "msg--->" + i));
        }
    }
}
