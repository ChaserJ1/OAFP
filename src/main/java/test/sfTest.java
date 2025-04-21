package test;

import oafp.faulttolerance.OAFPSFPlanner;
import oafp.model.OperatorNode;
import oafp.model.StreamTopology;

import java.util.Map;

/**
 * @Author: pengjia
 * @Description:
 */
public class sfTest {
    public static void main(String[] args) {
        StreamTopology topo = new StreamTopology();

        OperatorNode A = new OperatorNode("A", false);
        OperatorNode B = new OperatorNode("B", false);
        OperatorNode C = new OperatorNode("C", false);

        B.addUpstream(A);
        C.addUpstream(B);

        topo.addOperator(A);
        topo.addOperator(B);
        topo.addOperator(C);

        OAFPSFPlanner planner = new OAFPSFPlanner(topo, C, 0.95); // δ = 0.95
        Map<String, Double> result = planner.generatePlan();

        for (Map.Entry<String, Double> entry : result.entrySet()) {
            System.out.println("Task " + entry.getKey() + " -> ri = " + entry.getValue());
        }

    }
}
