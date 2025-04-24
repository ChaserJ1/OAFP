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

/**
 * 用于匹配单词并进行采样备份
 */
public class MatcherBolt extends BaseRichBolt {
    private OutputCollector collector;

    private final String taskId = "C";

    private double ri = 1.0;

    private final Random rand = new Random();

    /**
     * 初始化该bolt，该方法会在bolt启动时调用一次
     * @param stormConf
     * @param context
     * @param collector
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.ri = TaskRegistry.getRi(taskId); // 获取当前任务的采样率
    }

    /**
     * 整个处理逻辑需要重新规划一下，目前只是一个简单的根据固定的一个句子的输入来做决定
     * 后续进行相应调整
     * @param input
     */
    @Override
    public void execute(Tuple input) {
        // 如果任务失败，模拟从备份恢复
        if (FaultInjector.isFailed(taskId)) {
            // 获取该任务的备份数据
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

        // 从输入元组中获取相应单词字段
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