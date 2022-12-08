import java.util.Properties;
import java.util.concurrent.Future;

public class Producer {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer<String, String>(kafkaProps);
        ProducerRecord<String, String> record = new ProducerRecord<>("datajek-topic", "my-key", "my-value");
        try {
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata recordMetadata = future.get();
            System.out.println("Message written to partition with offset", recordMetadata.partition(), recordMetadata.offset());
        } catch(Exception e){
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
