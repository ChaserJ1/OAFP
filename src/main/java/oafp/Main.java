package oafp;

import oafp.experiment.RecoveryLogger;
import oafp.model.*;
import oafp.topology.WordCountTopology;
import oafp.faulttolerance.*;

import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.Config;

import java.util.Map;

public class Main {
    public static void main(String[] args) throws Exception {
        // 构造逻辑 DAG 拓扑（与物理拓扑相对应）
        StreamTopology topo = new StreamTopology();
        // 创建 operator 节点
        OperatorNode A = new OperatorNode("A", false); // SentenceSpout
        OperatorNode B = new OperatorNode("B", false); // KeywordFilter
        OperatorNode C = new OperatorNode("C", false); // Classifier
        OperatorNode D = new OperatorNode("D", true);  // Joiner（多输入）
        OperatorNode E = new OperatorNode("E", false); // CountBolt
        OperatorNode F = new OperatorNode("F", false); // Sink

        // 构建 DAG 结构
        B.addUpstream(A); // A → B
        C.addUpstream(A); // A → C
        D.addUpstream(B); // B → D
        D.addUpstream(C); // C → D
        E.addUpstream(A); // A → E
        F.addUpstream(D); // D → F
        F.addUpstream(E); // E → F

        // 添加到逻辑拓扑
        topo.addOperator(A);
        topo.addOperator(B);
        topo.addOperator(C);
        topo.addOperator(D);
        topo.addOperator(E);
        topo.addOperator(F);

        A.inputRate = 1000; // 模拟输入速率
        B.inputRate = 1000;
        C.inputRate = 1000;
        D.inputRate = 1000;
        E.inputRate = 1000;
        F.inputRate = 1000;

        // 调用 OAFP-SF，计算 ri
        OAFPSFPlanner planner = new OAFPSFPlanner(topo, F, 0.9);
        Map<String, Double> riMap = planner.generatePlan();

        System.out.println("=== [G3] OAFP-SF 分配结果为 ===");
        for (Map.Entry<String, Double> entry : riMap.entrySet()) {
            String taskId = entry.getKey();
            double ri = entry.getValue();
            TaskRegistry.setRi(taskId, ri); // 注册到运行时
            System.out.printf("任务 %s 分配 ri = %.2f\n", taskId, ri);
        }

        // 启动动态 ri 更新线程 每10s更新一次输入速率变化并且重新计算ri
        new Thread(() -> {
            try {
                while (true) {
                    // 模拟流量变化
                    double lambda = 800 + Math.random() * 400; // 模拟输入速率变化
                    A.inputRate = 800 + Math.random() * 400;
                    B.inputRate = 800 + Math.random() * 400;
                    C.inputRate = 800 + Math.random() * 400;
                    D.inputRate = 800 + Math.random() * 400;
                    E.inputRate = 800 + Math.random() * 400;
                    F.inputRate = 800 + Math.random() * 400;

                    // 每轮更新采样率
                    OAFPSFPlanner replanner = new OAFPSFPlanner(topo, F, 0.9);
                    Map<String, Double> newRiMap = replanner.generatePlan();
                    for (Map.Entry<String, Double> entry : newRiMap.entrySet()) {
                        TaskRegistry.setRi(entry.getKey(), entry.getValue());
                        System.out.printf("[动态调整] %s λ = %.2f → ri = %.2f\n",
                                entry.getKey(), topo.get(entry.getKey()).inputRate, entry.getValue());
                    }

                    Thread.sleep(10000); // 每 10 秒重新规划
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        // 输出评估模块
        RecoveryLogger.clear();
        RecoveryLogger.setRecovery(false);

        // 提交 Storm 拓扑
        TopologyBuilder builder = WordCountTopology.build();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("G3-Complex", new Config(), builder.createTopology());

        Thread.sleep(15000); // 模拟正常运行15秒

        // 模拟故障
        System.out.println(">>> 模拟任务 D 故障...");
        FaultInjector.fail("D");
        RecoveryLogger.setRecovery(true); // 开始记录恢复阶段输出

        // 模拟恢复
        FaultInjector.recover("D");
        RecoveryLogger.setRecovery(false);
        System.out.println(">>> D 已恢复");

        //输出日志
        Thread.sleep(5000);
        RecoveryLogger.exportResults("output/g3_result");
        double score = RecoveryLogger.jaccardSimilarity();
        System.out.printf(">>> 精度评估完成，Jaccard 相似度 = %.3f\n", score);



    }
}
