package oafp.faulttolerance;

import java.util.HashSet;
import java.util.Set;

/**
 * 故障注入器，用于模拟任务节点的故障和恢复。
 * 该类维护一个存储失败任务ID的集合，提供了使任务失败、恢复任务和检查任务是否失败的方法。
 * 后续需要对这个任务失败进行进一步的考虑，而不是简简单单的强制让某个任务失败
 */
public class FaultInjector {

    // 存储失败任务的ID集合
    private static final Set<String> failedTasks = new HashSet<>();

    // 使指定任务失败并将任务Id添加到失败任务集合中
    public static void fail(String taskId) {
        failedTasks.add(taskId);
    }

    // 从失败任务集合中移除指定任务，表示任务已恢复
    public static void recover(String taskId) {
        failedTasks.remove(taskId);
    }
    // 检查指定任务是否失败
    public static boolean isFailed(String taskId) {
        return failedTasks.contains(taskId);
    }
}
