package dk.ku.di.dms.vms.tpcc.common.datagen;

import java.util.Arrays;
import java.util.Random;

public final class DataGenUtils {

    private static final String numbers = "0123456789";
    private static final String alphanumeric = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    private static final char[] alphaNumericCharArray = alphanumeric.toCharArray();
    private static final int arrmax = 61;  /* index of last array element */
    private static final Random generate = new Random();

    /**
     * Based on <a href="https://github.com/AgilData/tpcc/blob/master/src/main/java/com/codefutures/tpcc/Util.java#L147">AgilData</a>
     * @param x Min length
     * @param y Max length
     * @return An alphanumeric string
     */
    public static String makeAlphaString(int x, int y) {
        StringBuilder str = new StringBuilder();
        char[] alphanum = Arrays.copyOf(alphaNumericCharArray, alphaNumericCharArray.length);
        int len = randomNumber(x, y);
        for (int i = 0; i < len; i++)
            str.append(alphanum[randomNumber(0, arrmax)]);
        return str.toString();
    }

    /**
     * Based on <a href="https://github.com/AgilData/tpcc/blob/master/src/main/java/com/codefutures/tpcc/Util.java#L96">AgilData</a>
     * @param min Min number
     * @param max Max number
     * @return A random number
     */
    public static int randomNumber(int min, int max) {
        int next = generate.nextInt();
        int div = next % ((max - min) + 1);
        if (div < 0) {
            div = div * -1;
        }
        return min + div;
    }

    private static final String[] n = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};

    /**
     * Based on <a href="https://github.com/AgilData/tpcc/blob/master/src/main/java/com/codefutures/tpcc/Util.java#L224">AgilData</a>
     * @param num Base number
     * @return A last name
     */
    public static String lastName(int num) {
        return n[num / 100] + n[(num / 10) % 10] + n[num % 10];
    }

    public static int nuRand(int A, int C, int x, int y) {
        return ((((randomNumber(0, A) | randomNumber(x, y)) + C) % (y - x + 1)) + x);
    }

    /**
     * Based on <a href="https://github.com/AgilData/tpcc/blob/master/src/main/java/com/codefutures/tpcc/Util.java#L170">AgilData</a>
     * @param x Min size
     * @param y Max size
     * @return Number in string format
     */
    public static String makeNumberString(int x, int y) {
        StringBuilder str = new StringBuilder();
        char[] numeric = numbers.toCharArray();
        int arrmax = numbers.length() - 1;
        int i;
        int len;
        len = randomNumber(x, y);
        for (i = 0; i < len; i++)
            str.append(numeric[randomNumber(0, arrmax)]);
        return str.toString();
    }

}