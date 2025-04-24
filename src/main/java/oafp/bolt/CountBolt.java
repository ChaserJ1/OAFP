package oafp.bolt;

import oafp.faulttolerance.ApproxBackupManager;
import oafp.faulttolerance.FaultInjector;
import oafp.model.TaskRegistry;

import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class CountBolt extends BaseRichBolt {
    private OutputCollector collector;
    private final String taskId = "E";
    private final Random rand = new Random();
    private Map<String, Integer> counts = new HashMap<>();

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        if (FaultInjector.isFailed(taskId)) {
            List<String> backups = ApproxBackupManager.getInstance().getBackup(taskId);
            if (backups != null) {
                for (String word : backups) {
                    emitCount(word);
                }
            }
            return;
        }

        String sentence = input.getStringByField("sentence");
        for (String word : sentence.split(" ")) {
            if (rand.nextDouble() <= TaskRegistry.getRi(taskId)) {
                ApproxBackupManager.getInstance().backup(taskId, word);
            }
            emitCount(word);
        }
    }

    private void emitCount(String word) {
        int count = counts.getOrDefault(word, 0) + 1;
        counts.put(word, count);
        collector.emit(new Values(word, count));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
