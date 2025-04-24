package oafp.model;

import java.util.ArrayList;
import java.util.List;

/**
 * 表示一个任务节点
 */
public class OperatorNode {
    // 任务节点Id
    public String id;
    // 是否为Join节点
    public boolean isJoinOperator;
    // 存储该节点的上游节点列表
    public List<OperatorNode> upstream = new ArrayList<>();
    // 存储该节点的下游节点列表
    public List<OperatorNode> downstream = new ArrayList<>();

    /**
     * 模拟参数，后续需要完善更改
     */
    public double inputRate = 1000.0;
    public double checkpointInterval = 5.0; // 秒
    public double checkpointLatencyPerUnit = 0.001; // 模拟参数
    public double samplingRatio = 1.0; // ri

    public OperatorNode(String id, boolean isJoin) {
        this.id = id;
        this.isJoinOperator = isJoin;
    }

    /**
     * 添加该节点的上游节点，并且更新上下游节点关系
     * @param parent
     */
    public void addUpstream(OperatorNode parent) {
        this.upstream.add(parent);
        parent.downstream.add(this);
    }
}
