package oafp.bolt;

import oafp.faulttolerance.ApproxBackupManager;
import oafp.faulttolerance.FaultInjector;
import oafp.model.TaskRegistry;

import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class KeywordFilterBolt extends BaseRichBolt {
    private OutputCollector collector;
    private final String taskId = "B";
    private final Random rand = new Random();

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        if (FaultInjector.isFailed(taskId)) {
            List<String> backups = ApproxBackupManager.getInstance().getBackup(taskId);
            if (backups != null) {
                for (String word : backups) {
                    collector.emit(new Values(word));
                }
            }
            return;
        }

        String sentence = input.getStringByField("sentence");
        if (sentence.contains("storm")) {
            double ri = TaskRegistry.getRi(taskId);
            if (rand.nextDouble() <= ri) {
                ApproxBackupManager.getInstance().backup(taskId, sentence);
            }
            collector.emit(new Values("storm")); // 简化处理
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
