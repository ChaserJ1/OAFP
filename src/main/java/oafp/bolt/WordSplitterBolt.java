package oafp.bolt;

import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.tuple.Fields;

import java.util.Map;

public class WordSplitterBolt extends BaseRichBolt {
    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String sentence = input.getStringByField("sentence");
        for (String word : sentence.split(" ")) {
            collector.emit(new Values(word));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
