package oafp.faulttolerance;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ApproxBackupManager {
    private static final ApproxBackupManager instance = new ApproxBackupManager();

    private final Map<String, ReservoirSampler<String>> backupMap = new HashMap<>();
    private final int defaultCapacity = 10; // 默认采样容量（可调节）

    private ApproxBackupManager() {}

    public static ApproxBackupManager getInstance() {
        return instance;
    }

    public void backup(String taskId, String item) {
        ReservoirSampler<String> sampler = backupMap.get(taskId);
        if (sampler == null) {
            sampler = new ReservoirSampler<String>(defaultCapacity);
            backupMap.put(taskId, sampler);
        }
        sampler.add(item);
    }

    public List<String> getBackup(String taskId) {
        ReservoirSampler<String> sampler = backupMap.get(taskId);
        return (sampler != null) ? sampler.getSamples() : null;
    }

    public void clear(String taskId) {
        ReservoirSampler<String> sampler = backupMap.get(taskId);
        if (sampler != null) {
            sampler.clear();
        }
    }

    public void clearAll() {
        for (ReservoirSampler<String> sampler : backupMap.values()) {
            sampler.clear();
        }
    }
}
