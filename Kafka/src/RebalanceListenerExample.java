import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class RebalanceListenerExample {
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    class RebalanceHandler implements ConsumerRebalanceListener {
        KafkaConsumer<String, String> consumer;
        RebalanceHandler(KafkaConsumer<String, String> consumer){
            this.consumer = consumer;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection){
            consumer.commitSync(currentOffsets);
        }
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions){
            for(TopicPartition partition : partitions){
                long offset = readOffsetFromDB(partition);
                consumer.seek(partition, offset);
            }

            long readOffsetFromDB(TopicPartition partition){
                return 0;
            }
        }

    }

    public void run(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("auto.offset.reset", "earliest");
        props.put("group.id", "DatajekConsumers");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Pattern.compile("datajek-.*"), new RebalanceHandler(consumer));
        try {
            while(true){
                Duration oneSecond = Duration.ofMillis(1000);
                ConsumerRecords<String, String> records = consumer.poll(oneSecond);
                for (ConsumerRecord<String, String> record: records) {
                   TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                   OffsetAndMetadata metadata = new OffsetAndMetadata(record.offset() + 1, "no metadata");
                   currentOffsets.put(topicPartition, metadata);
                }
                consumer.commitAsync();
            } catch (Exception e){

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
