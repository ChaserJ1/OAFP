package oafp.faulttolerance;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 水库采样器，用于对数据流进行采样
 * @param <T> 采样数据的类型
 */
public class ReservoirSampler<T> {
    // 采样容量
    private final int capacity;

    // 存储采样数据的列表
    private final List<T> samples;

    // 数据流中的数据总数
    private int totalCount;
    private final Random random;

    public ReservoirSampler(int capacity) {
        this.capacity = capacity;
        this.samples = new ArrayList<T>(capacity);
        this.random = new Random();
        this.totalCount = 0;
    }

    /**
     * 向采样器中添加一个item
     */
    public void add(T item) {
        totalCount++;
        // 如果采样列表的大小小于容量，直接将item添加到列表中
        if (samples.size() < capacity) {
            samples.add(item);
        } else {
            int r = random.nextInt(totalCount);
            // 如果随机数小于容量，则随机替换一个列表中的项目
            if (r < capacity) {
                samples.set(r, item);
            }
        }
    }

    /**
     * 获取采样器中的采样数据
     */
    public List<T> getSamples() {
        return new ArrayList<T>(samples);
    }

    public void clear() {
        samples.clear();
        totalCount = 0;
    }
}
