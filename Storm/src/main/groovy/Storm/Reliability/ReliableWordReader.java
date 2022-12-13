package Storm.Reliability;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReliableWordReader extends BaseRichSpout {

    private static final Integer MAX_FAILS = 3;
    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private BufferedReader reader;

    private Map<Integer, String> allMessages;

    private List<Integer> toSend;

    private boolean completed = false;
    private HashMap<Integer, Integer> msgFailureCount;

    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        try {
            this.fileReader = new FileReader(map.get("fileToRead").toString());
            this.reader = new BufferedReader(fileReader);
            this.allMessages = new HashMap<Integer, String>();
            this.toSend = new ArrayList<Integer>();
            int i = 0;
            while(reader.readLine() != null){
                allMessages.put(i++, reader.readLine());
                toSend.add(i);
            }
            this.msgFailureCount = new HashMap<Integer, Integer>();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void nextTuple() {
        if(!toSend.isEmpty()){
            for (int msgId: toSend
                 ) {
                String word = allMessages.get(msgId);
                collector.emit(new Values(word), msgId);
            }
            toSend.clear();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

    public void ack(Object msgId){
        System.out.println("successful");
    }

    public void fail(Object msgId){
        Integer failedId = (Integer) msgId;
        Integer failures = 1;

        if(msgFailureCount.containsKey(failedId)) {
            failures = msgFailureCount.get(failedId)+1;
        }

        if(failures < MAX_FAILS){
            msgFailureCount.put(failedId, failures);
            toSend.add(failedId);
            System.out.println("re-sending");
        }
        else {
            System.out.println("failed");
        }
    }
}
