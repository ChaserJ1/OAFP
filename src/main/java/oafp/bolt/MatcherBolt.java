package oafp.bolt;

import oafp.faulttolerance.ApproxBackupManager;
import oafp.faulttolerance.FaultInjector;
import oafp.model.TaskRegistry;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import oafp.faulttolerance.ApproxBackupManager;
import oafp.model.OperatorNode;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class MatcherBolt extends BaseRichBolt {
    private OutputCollector collector;

    private final String taskId = "C";

    private double ri = 1.0;

    private final Random rand = new Random();


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.ri = TaskRegistry.getRi(taskId);
    }

    @Override
    public void execute(Tuple input) {

        // 如果任务失败，模拟从备份恢复
        if (FaultInjector.isFailed(taskId)) {
            List<String> backups = ApproxBackupManager.getInstance().getBackup(taskId);
            if (backups != null) {
                for (String data : backups) {
                    System.out.println("[恢复] " + taskId + " 重新处理数据: " + data);
                    collector.emit(new Values(data));
                }
            }

            return; // 故障时不再处理新输入
        }
        System.out.println("[恢复] " + taskId + " 恢复完成");
        String word = input.getStringByField("word");

        if ("storm".equalsIgnoreCase(word)) {
            // 控制采样是否触发
            if (rand.nextDouble() <= ri) {
                ApproxBackupManager.getInstance().backup(taskId, word);
            }

            collector.emit(new Values(word));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("matched"));
    }


}