package oafp.bolt;

import oafp.faulttolerance.ApproxBackupManager;
import oafp.faulttolerance.FaultInjector;
import oafp.model.TaskRegistry;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.tuple.Fields;

import java.util.*;


public class WordCounterBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> counts;
    private final String taskId = "C"; // 任务标识
    private final Random rand = new Random();

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.counts = new HashMap<>();
    }

    public void execute(Tuple input) {
        if (FaultInjector.isFailed(taskId)) {
            List<String> backups = ApproxBackupManager.getInstance().getBackup(taskId);
            if (backups != null) {
                for (String word : backups) {
                    processWord(word);
                }
            }
            return;
        }

        // 动态实时获取采样率
        double ri = TaskRegistry.getRi(taskId);

        String word = input.getStringByField("word");
        if (rand.nextDouble() <= ri) {
            ApproxBackupManager.getInstance().backup(taskId, word);
        }

        processWord(word);
    }

    private void processWord(String word) {
        int count = counts.getOrDefault(word, 0) + 1;
        counts.put(word, count);
        collector.emit(new Values(word, count));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
/*public class WordCounterBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> counts;
    private final String taskId = "C";
    private double ri;
    private final Random rand = new Random();

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.counts = new HashMap<>();
        this.ri = TaskRegistry.getRi(taskId);
    }

    public void execute(Tuple input) {
        if (FaultInjector.isFailed(taskId)) {
            List<String> backups = ApproxBackupManager.getInstance().getBackup(taskId);
            if (backups != null) {
                for (String word : backups) {
                    processWord(word);
                }
            }
            return;
        }

        String word = input.getStringByField("word");
        if (rand.nextDouble() <= ri) {
            ApproxBackupManager.getInstance().backup(taskId, word);
        }

        processWord(word);
    }

    private void processWord(String word) {
        int count = counts.getOrDefault(word, 0) + 1;
        counts.put(word, count);
        collector.emit(new Values(word, count));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}*/
