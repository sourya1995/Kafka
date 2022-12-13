package Storm.WordCount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.graalvm.compiler.word.Word;

public class TopologyMain {
    public static void main(String[] args) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("word-reader", new WordReader());
        topologyBuilder.setBolt("word-counter", new WordCounter())
                .shuffleGrouping("word-reader");

        Config config = new Config();
        config.put("fileToRead", "filePathOfWords");
        config.put("dirToWrite", "pathWhereToWriteStuff");

        LocalCluster localCluster = new LocalCluster();
        try {
            localCluster.submitTopology("WordCounter-Topology", config, topologyBuilder.createTopology());
        } finally {
            localCluster.shutdown();
        }
        Thread.sleep(10000);

    }
}
