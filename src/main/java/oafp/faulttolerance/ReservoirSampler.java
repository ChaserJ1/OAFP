package oafp.faulttolerance;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ReservoirSampler<T> {
    private final int capacity;
    private final List<T> samples;
    private int totalCount;
    private final Random random;

    public ReservoirSampler(int capacity) {
        this.capacity = capacity;
        this.samples = new ArrayList<T>(capacity);
        this.random = new Random();
        this.totalCount = 0;
    }

    public void add(T item) {
        totalCount++;
        if (samples.size() < capacity) {
            samples.add(item);
        } else {
            int r = random.nextInt(totalCount);
            if (r < capacity) {
                samples.set(r, item);
            }
        }
    }

    public List<T> getSamples() {
        return new ArrayList<T>(samples);
    }

    public void clear() {
        samples.clear();
        totalCount = 0;
    }
}
