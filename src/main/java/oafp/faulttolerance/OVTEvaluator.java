package oafp.faulttolerance;

import oafp.model.OperatorNode;
import oafp.model.StreamTopology;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 评估当某个任务失败时，拓扑的整体输出有效性OVT
 */
public class OVTEvaluator {
    private final StreamTopology topology;
    private final OperatorNode sink;

    public OVTEvaluator(StreamTopology topology, OperatorNode sink) {
        this.topology = topology;
        this.sink = sink;
    }

    /**
     * 假设某个 task 失败，计算该情况下的整体输出有效性
     */
    public double evaluateOVTWithFailure(String failedTaskId) {
        // 首先查找所有节点到sink节点的路径
        List<List<OperatorNode>> paths = topology.findAllPathsToSink(sink);

        double total = 0.0;
        // 计算每条路径的输出有效性
        for (List<OperatorNode> path : paths) {
            double pathDV = 1.0; // 初始为1.0，即100%

            // 计算路径上每个节点的输出有效性
            for (OperatorNode node : path) {
                // 如果为失败节点，则有效性为采样率
                if (node.id.equals(failedTaskId)) {
                    pathDV *= node.samplingRatio;
                } else {
                    pathDV *= 1.0;
                }
            }
            total += pathDV;
        }
        return total / paths.size(); // 以平均值作为整体的输出有效性
    }
}
