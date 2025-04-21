package oafp.model;

import java.util.ArrayList;
import java.util.List;

/**
 * 表示一个任务节点
 */
public class OperatorNode {
    public String id;
    public boolean isJoinOperator;
    public List<OperatorNode> upstream = new ArrayList<>();
    public List<OperatorNode> downstream = new ArrayList<>();

    public double inputRate = 1000.0;
    public double checkpointInterval = 5.0; // 秒
    public double checkpointLatencyPerUnit = 0.001; // 模拟参数
    public double samplingRatio = 1.0; // ri

    public OperatorNode(String id, boolean isJoin) {
        this.id = id;
        this.isJoinOperator = isJoin;
    }

    public void addUpstream(OperatorNode parent) {
        this.upstream.add(parent);
        parent.downstream.add(this);
    }
}
