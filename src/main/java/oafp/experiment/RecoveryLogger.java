package oafp.experiment;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class RecoveryLogger {
    private static final List<String> baselineOutputs = new ArrayList<>();
    private static final List<String> recoveredOutputs = new ArrayList<>();
    private static boolean inRecovery = false;

    public static void setRecovery(boolean recoveryMode) {
        inRecovery = recoveryMode;
    }

    public static void log(String value) {
        if (inRecovery) {
            recoveredOutputs.add(value);
        } else {
            baselineOutputs.add(value);
        }
    }

    public static void exportResults(String pathPrefix) throws IOException {
        FileWriter writer = new FileWriter(pathPrefix + "_baseline.csv");
        for (String out : baselineOutputs) {
            writer.write(out + "\n");
        }
        writer.close();

        writer = new FileWriter(pathPrefix + "_recovered.csv");
        for (String out : recoveredOutputs) {
            writer.write(out + "\n");
        }
        writer.close();
    }

    public static double jaccardSimilarity() {
        Set<String> base = new HashSet<>(baselineOutputs);
        Set<String> rec = new HashSet<>(recoveredOutputs);
        Set<String> intersection = new HashSet<>(base);
        intersection.retainAll(rec);

        Set<String> union = new HashSet<>(base);
        union.addAll(rec);

        return union.size() == 0 ? 1.0 : (double) intersection.size() / union.size();
    }

    public static void clear() {
        baselineOutputs.clear();
        recoveredOutputs.clear();
    }
}
