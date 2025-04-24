package oafp.faulttolerance;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 近似备份管理类，用于管理任务的采样备份数据
 */
public class ApproxBackupManager {
    private static final ApproxBackupManager instance = new ApproxBackupManager();

    // 存储每个任务的采样器
    private final Map<String, ReservoirSampler<String>> backupMap = new HashMap<>();

    private final int defaultCapacity = 10; // 默认采样容量，根据任务量可调整

    private ApproxBackupManager() {}

    /**
     * 获取ApproxBackupManager的单例实例
     */
    public static ApproxBackupManager getInstance() {
        return instance;
    }

    /**
     * 为指定taskId的任务进行采样备份
     * @param taskId 任务ID
     * @param item 要备份的项目
     */
    public void backup(String taskId, String item) {
        // 先根据任务id获取相应的采样器
        ReservoirSampler<String> sampler = backupMap.get(taskId);
        if (sampler == null) {
            sampler = new ReservoirSampler<>(defaultCapacity);
            backupMap.put(taskId, sampler);
        }
        sampler.add(item);
    }

    /**
     * 获取指定任务ID的任务的备份数据
     * @param taskId 任务Id
     * @return 返回该任务的备份数据列表，如果不存在则返回null
     */
    public List<String> getBackup(String taskId) {
        ReservoirSampler<String> sampler = backupMap.get(taskId);
        return (sampler != null) ? sampler.getSamples() : null;
    }

    /**
     * 清除指定任务Id的任务的备份数据
     * @param taskId 任务Id
     */
    public void clear(String taskId) {
        ReservoirSampler<String> sampler = backupMap.get(taskId);
        if (sampler != null) {
            sampler.clear();
        }
    }

    /**
     * 清除所有任务的备份数据
     */
    public void clearAll() {
        for (ReservoirSampler<String> sampler : backupMap.values()) {
            sampler.clear();
        }
    }
}
