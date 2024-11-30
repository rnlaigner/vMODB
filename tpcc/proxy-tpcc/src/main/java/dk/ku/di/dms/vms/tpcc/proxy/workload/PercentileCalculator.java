package dk.ku.di.dms.vms.tpcc.proxy.workload;

import java.util.List;

public final class PercentileCalculator {

    /**
     * The data must be sorted
     */
    public static double calculatePercentile(List<Long> data, double percentile) {
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("Data array must not be null or empty.");
        }
        if (percentile < 0 || percentile > 1) {
            throw new IllegalArgumentException("Percentile must be between 0 and 1.");
        }

        double rank = percentile * (data.size() - 1);

        int lowerIndex = (int) Math.floor(rank);
        int upperIndex = (int) Math.ceil(rank);

        if (lowerIndex == upperIndex) {
            return data.get(lowerIndex);
        } else {
            double weight = rank - lowerIndex;
            return data.get(lowerIndex) * (1 - weight) + data.get(upperIndex) * weight;
        }
    }

}

