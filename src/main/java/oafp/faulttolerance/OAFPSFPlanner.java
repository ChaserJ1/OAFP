package oafp.faulttolerance;

import oafp.model.OperatorNode;
import oafp.model.StreamTopology;

import java.util.HashMap;
import java.util.Map;

/**
 * 实现贪心算法，对于单节点错误，为每个task计算最小的ri，满足OVT >= δ
 */
public class OAFPSFPlanner {
    private final StreamTopology topology;
    private final OperatorNode sink;
    private final double delta; // 用户定义的准确性阈值 δ
    private final double step = 0.05; // ri 递减步长

    public OAFPSFPlanner(StreamTopology topology, OperatorNode sink, double delta) {
        this.topology = topology;
        this.sink = sink;
        this.delta = delta;
    }

    /**
     * 生成采样率规划方案。
     * 遍历拓扑中的所有操作节点，通过贪心算法递减每个节点的采样率，
     * 直到整体输出有效性 OVT 小于 δ 时，回退一步并记录该节点的最优采样率。
     * @return 包含每个任务节点ID和其最优采样率的映射
     */
    public Map<String, Double> generatePlan() {
        Map<String, Double> samplingRatios = new HashMap<>();

        OVTEvaluator evaluator = new OVTEvaluator(topology, sink);

        for (OperatorNode op : topology.getAllOperators()) {
            // 初始默认 ri = 1.0
            double ri = 1.0;
            op.samplingRatio = ri;

            // 贪心递减 ri，直到 OVT < δ（再回退一步）
            while (ri >= 0.05) {
                op.samplingRatio = ri;
                double ovt = evaluator.evaluateOVTWithFailure(op.id);
                if (ovt >= delta) {
                    ri -= step;
                } else {
                    ri += step; // 回退最后一步
                    break;
                }
            }

            if (ri < 0.05) ri = 0.05; // 设置最小下限

            op.samplingRatio = ri;

            // 将节点ID和其最优采样率存入映射
            samplingRatios.put(op.id, ri);
        }

        return samplingRatios;
    }
}
