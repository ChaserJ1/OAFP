package oafp.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
/**
 * 句子数据源，用于生成句子数据并发送到拓扑中。周期性地发送预定义的句子数据。
 */
public class SentenceSpout extends BaseRichSpout {
    // 输出收集器，用于发送数据
    private SpoutOutputCollector collector;

    // 预定义的句子数组，后续要采用真实数据集
    private String[] sentences = new String[]{
        "storm is fast", "i love storm", "hello world", "distributed stream processing"
    };

    // 当前发送的句子索引
    private int index = 0;

    /**
     * 打开数据源，初始化输出收集器。
     * @param conf 配置信息
     * @param context 拓扑上下文
     * @param collector 输出收集器
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    /**
     * 发送下一个数据元组。
     * 周期性地发送预定义的句子数据，并更新索引。
     */
    @Override
    public void nextTuple() {
        this.collector.emit(new Values(sentences[index]));
        index = (index + 1) % sentences.length;
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("[Spout] 发送数据: " + sentences[index]);
    }

    /**
     * 声明输出字段。
     * 指定输出数据的字段名称为 "sentence"。
     * @param declarer 输出字段声明器
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }
}