package oafp.model;

import java.util.HashMap;
import java.util.Map;

public class TaskRegistry {
    private static final Map<String, Double> samplingMap = new HashMap<>();

    public static void setRi(String taskId, double ri) {
        samplingMap.put(taskId, ri);
    }

    public static double getRi(String taskId) {
        return samplingMap.getOrDefault(taskId, 1.0);
    }
}
