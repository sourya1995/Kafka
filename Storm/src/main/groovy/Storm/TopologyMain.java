package Storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {
    public static void main(String[] args) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("Yahoo-Finance-Spout", new yfSpout());
        topologyBuilder.setBolt("Yahoo-FInance-Bolt", new yfBolt())
                .shuffleGrouping("Yahoo-Finance-Spout");

        StormTopology topology = topologyBuilder.createTopology();
        Config config = new Config();
        config.setDebug(true);
        config.put("fileToWrite", "Users/johnw/OneDrive/Desktop/yfOutput/output.txt");

        LocalCluster localCluster = new LocalCluster();
        try {
            StormSubmitter.submitTopology("Stock-Tracker-Topology", config, topology);
            Thread.sleep(10000);
        } finally {
            localCluster.shutdown();
        }

    }
}
