package oafp;

import oafp.faulttolerance.OAFPSFPlanner;
import oafp.model.OperatorNode;
import oafp.model.StreamTopology;

import java.util.Map;

public class OAFPSFExample {
    public static void main(String[] args) {
        // Step 1: 构建拓扑 G1: A -> B -> C
        StreamTopology topo = new StreamTopology();

        OperatorNode A = new OperatorNode("A", false); // Spout
        OperatorNode B = new OperatorNode("B", false); // Parser
        OperatorNode C = new OperatorNode("C", false); // Matcher

        B.addUpstream(A); // A -> B
        C.addUpstream(B); // B -> C

        topo.addOperator(A);
        topo.addOperator(B);
        topo.addOperator(C);

        // Step 2: 设置参数（默认输入速率、采样率初始为 1.0）
        A.inputRate = 1000;
        B.inputRate = 1000;
        C.inputRate = 1000;

        // Step 3: 执行 OAFP-SF 算法，设置准确性阈值 δ
        double delta = 0.95;
        OAFPSFPlanner planner = new OAFPSFPlanner(topo, C, delta);
        Map<String, Double> samplingPlan = planner.generatePlan();

        // Step 4: 打印结果
        System.out.println("=== OAFP-SF 采样率规划结果 ===");
        for (Map.Entry<String, Double> entry : samplingPlan.entrySet()) {
            System.out.printf("任务 %s 最优 ri = %.2f\n", entry.getKey(), entry.getValue());
        }
    }
}
