import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

public class StoppingConsumerMainThread {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("auto.offset.reset", "earliest");
        props.put("group.id", "DatajekConsumers");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Pattern.compile("datajek-.*"));
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run() {
                consumer.wakeup();
                try{
                    mainThread.join();
                } catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        });

        while(true){
            Duration oneSecond = Duration.ofMillis(1000);
            ConsumerRecords<String, String> records = consumer.poll(oneSecond);
            for (ConsumerRecord<String, String> record: records) {
                TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                OffsetAndMetadata metadata = new OffsetAndMetadata(record.offset() + 1, "no metadata");
                currentOffsets.put(topicPartition, metadata);
            }
            consumer.commitAsync();
        } catch (WakeupException e){

        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }
}
    }
}
