package Storm.TridentStreams;

import Storm.WordCount.WordReader;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.tuple.Fields;

public class helloWorldTrident {
    public static void main(String[] args) {
        TridentTopology tridentTopology = new TridentTopology();
        tridentTopology.newStream("lines", new WordReader())
                .each(new Fields("word"),
                        new splitFunction(),
                        new Fields("word_split"))
                .groupBy(new Fields("word_split"))


    }
}
