import javax.security.auth.callback.Callback;
import java.util.Properties;
import java.util.concurrent.Future;

public class AsyncProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer<String, String>(kafkaProps);
        ProducerRecord<String, String> record = new ProducerRecord<>("datajek-topic", "my-key", "my-value");
        try {
            producer.send(record, new ProducerCallback());


        } catch(Exception e){
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
        }

        Thread.sleep(3000);
    }

    private static class ProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e){
            if(e != null){
                e.printStackTrace();
            }
            else {
                System.out.println("Message written to partition with offset", recordMetadata.partition(), recordMetadata.offset());
            }
        }
    }
}
