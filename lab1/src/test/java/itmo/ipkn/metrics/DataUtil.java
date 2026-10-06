package itmo.ipkn.metrics;

import java.util.Arrays;
import java.util.Random;

import static java.lang.Math.pow;

public class DataUtil {

    private DataUtil() {
    }

    public static void initValuesByZipfLaw(long[] testValues) {
        final long seed = 67;
        final double kf = 1.15;
        final int maxValue = 1024;

        double[] cdf = new double[maxValue];
        double sum = 0;
        for (int base = 1; base <= maxValue; base++) {
            sum += 1.0 / pow(base, kf);
            cdf[base - 1] = sum;
        }

        for (int i = 0; i < maxValue; i++) {
            cdf[i] /= sum;
        }

        Random random = new Random(seed);
        for (int i = 0; i < testValues.length; i++) {
            int insertionPoint = Arrays.binarySearch(cdf, random.nextDouble());
            if (insertionPoint < 0) {
                insertionPoint = -insertionPoint - 1;
            }
            testValues[i] = insertionPoint + 1;
        }
    }

}
