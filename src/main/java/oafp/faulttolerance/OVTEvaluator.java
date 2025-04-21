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
        List<List<OperatorNode>> paths = topology.findAllPathsToSink(sink);
        double total = 0.0;
        for (List<OperatorNode> path : paths) {
            double pathDV = 1.0;
            for (OperatorNode node : path) {
                if (node.id.equals(failedTaskId)) {
                    pathDV *= node.samplingRatio; // βi = ri if failed
                } else {
                    pathDV *= 1.0; // βi = 1.0 if alive
                }
            }
            total += pathDV;
        }
        return total / paths.size(); // mean of all DV_out at sink
    }
}
