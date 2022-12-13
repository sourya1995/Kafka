package Storm;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;

public class yfBolt extends BaseBasicBolt {

    private PrintWriter writer;

    public void prepare(Map stormConf, TopologyContext context){
        String fileName = stormConf.get("fileToWrite").toString();
        try {
            this.writer = new PrintWriter(fileName, "UTF-8");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String symbol = tuple.getValue(0).toString();
        String timestamp = tuple.getString(1);
        Double price = (Double) tuple.getValueByField("price");
        Double prev_close = tuple.getDoubleByField("prev_close");
        Boolean gain = true;

        if(price <= prev_close){
            gain = false;
        }

        basicOutputCollector.emit(new Values(symbol, timestamp, price, gain));
        writer.println(symbol+", "+timestamp+", "+price+", "+gain);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("company", "timestamp", "price", "gain"));
    }

    public void cleanup(){
        writer.close();
    }
}
