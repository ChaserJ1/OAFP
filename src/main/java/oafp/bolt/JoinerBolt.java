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

public class JoinerBolt extends BaseRichBolt {
    private OutputCollector collector;
    private final String taskId = "D";
    private Map<String, String> wordBuffer = new HashMap<>();
    private Map<String, String> categoryBuffer = new HashMap<>();

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        if (FaultInjector.isFailed(taskId)) {
            List<String> backups = ApproxBackupManager.getInstance().getBackup(taskId);
            if (backups != null) {
                for (String joined : backups) {
                    collector.emit(new Values(joined));
                }
            }
            return;
        }

        String sourceStream = input.getSourceComponent();
        double ri = TaskRegistry.getRi(taskId);

        if (sourceStream.equals("B")) {
            String word = input.getStringByField("word");
            wordBuffer.put("latest", word);
        } else if (sourceStream.equals("C")) {
            String category = input.getStringByField("category");
            categoryBuffer.put("latest", category);
        }

        if (wordBuffer.containsKey("latest") && categoryBuffer.containsKey("latest")) {
            String joined = wordBuffer.get("latest") + "-" + categoryBuffer.get("latest");
            if (Math.random() <= ri) {
                ApproxBackupManager.getInstance().backup(taskId, joined);
            }
            collector.emit(new Values(joined));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("joined"));
    }
}
