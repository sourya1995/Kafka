import javax.xml.validation.Schema;
import java.util.Properties;

public class ProducerWithSerialization {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");
        KafkaProducer producer = new KafkaProducer(props);

        String key = "key1";
        String userSchema = "{\"type\":\"record\"," +
                "\"name\":\"Car\"," +
                "\"fields\":[{\"name\":\"brand\", \"type\":\"string\"}]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);

        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("brand", "Mercedes");

        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("datajek-topic", key, avroRecord);
        try {
            producer.send(record);
        } catch(SerializationException e){
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
