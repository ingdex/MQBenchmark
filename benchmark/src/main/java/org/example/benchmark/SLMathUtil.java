package org.example.benchmark;

public class SLMathUtil {
    public static int getNumberOfDigits(int num) {
        if (num == 0) {
            return 1;
        }
        num = num < 0 ? -num : num;
        return (int) Math.log10(num) + 1;
    }
}
