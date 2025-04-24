package oafp.faulttolerance;

import java.util.HashSet;
import java.util.Set;

public class FaultInjector {
    private static final Set<String> failedTasks = new HashSet<>();

    public static void fail(String taskId) {
        failedTasks.add(taskId);
    }

    public static void recover(String taskId) {
        failedTasks.remove(taskId);
    }

    public static boolean isFailed(String taskId) {
        return failedTasks.contains(taskId);
    }
}
