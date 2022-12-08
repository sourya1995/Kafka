import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

public class ConsumerExample {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("auto.offset.reset", "earliest");
        props.put("group.id", "DatajekConsumers");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Pattern.compile("datajek-.*"));

        try {
            while(true){
                Duration oneSecond = Duration.ofMillis(1000);
                ConsumerRecords<String, String> records = consumer.poll(oneSecond);
                for (ConsumerRecord<String, String> record: records) {
                    String topic = record.topic();
                    int partition = record.partition();
                    long recordOffset = record.offset();
                    String key = record.key();
                    String value = record.value();
                }
            }
        }
    }
}
