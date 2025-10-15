package org.mahmoud.lynxes.util;

/**
 * Utility class for formatting data into human-readable strings.
 * Provides methods for formatting bytes, durations, and other data types.
 */
public class FormatUtils {
    
    private static final String[] BYTE_UNITS = {"B", "KB", "MB", "GB", "TB", "PB", "EB"};
    private static final long BYTES_PER_UNIT = 1024;
    
    /**
     * Formats bytes to human-readable format.
     * Examples: 1024 -> "1.0 KB", 1536 -> "1.5 KB", 1048576 -> "1.0 MB"
     * 
     * @param bytes The number of bytes
     * @return Human-readable string
     */
    public static String formatBytes(long bytes) {
        if (bytes < 0) {
            return "0 B";
        }
        
        if (bytes < BYTES_PER_UNIT) {
            return bytes + " B";
        }
        
        int exp = (int) (Math.log(bytes) / Math.log(BYTES_PER_UNIT));
        if (exp >= BYTE_UNITS.length) {
            exp = BYTE_UNITS.length - 1;
        }
        
        String unit = BYTE_UNITS[exp];
        double value = bytes / Math.pow(BYTES_PER_UNIT, exp);
        
        return String.format("%.1f %s", value, unit);
    }
    
    /**
     * Formats bytes to human-readable format with custom precision.
     * 
     * @param bytes The number of bytes
     * @param precision The number of decimal places
     * @return Human-readable string
     */
    public static String formatBytes(long bytes, int precision) {
        if (bytes < 0) {
            return "0 B";
        }
        
        if (bytes < BYTES_PER_UNIT) {
            return bytes + " B";
        }
        
        int exp = (int) (Math.log(bytes) / Math.log(BYTES_PER_UNIT));
        if (exp >= BYTE_UNITS.length) {
            exp = BYTE_UNITS.length - 1;
        }
        
        String unit = BYTE_UNITS[exp];
        double value = bytes / Math.pow(BYTES_PER_UNIT, exp);
        
        return String.format("%." + precision + "f %s", value, unit);
    }
    
    /**
     * Formats duration to human-readable format.
     * Examples: 500 -> "500 ms", 1500 -> "1 s", 65000 -> "1 m 5 s"
     * 
     * @param milliseconds The duration in milliseconds
     * @return Human-readable string
     */
    public static String formatDuration(long milliseconds) {
        if (milliseconds < 0) {
            return "0 ms";
        }
        
        if (milliseconds < 1000) {
            return milliseconds + " ms";
        }
        
        long seconds = milliseconds / 1000;
        if (seconds < 60) {
            return seconds + " s";
        }
        
        long minutes = seconds / 60;
        if (minutes < 60) {
            long remainingSeconds = seconds % 60;
            return minutes + " m " + remainingSeconds + " s";
        }
        
        long hours = minutes / 60;
        long remainingMinutes = minutes % 60;
        return hours + " h " + remainingMinutes + " m";
    }
    
    /**
     * Formats duration to human-readable format with seconds precision.
     * 
     * @param milliseconds The duration in milliseconds
     * @return Human-readable string with seconds precision
     */
    public static String formatDurationSeconds(long milliseconds) {
        if (milliseconds < 0) {
            return "0.0 s";
        }
        
        double seconds = milliseconds / 1000.0;
        return String.format("%.1f s", seconds);
    }
    
    /**
     * Formats a percentage value.
     * 
     * @param value The value (0.0 to 1.0)
     * @return Formatted percentage string
     */
    public static String formatPercentage(double value) {
        if (value < 0) {
            return "0.0%";
        }
        if (value > 1) {
            return "100.0%";
        }
        
        return String.format("%.1f%%", value * 100);
    }
    
    /**
     * Formats a number with thousands separators.
     * 
     * @param number The number to format
     * @return Formatted number string
     */
    public static String formatNumber(long number) {
        return String.format("%,d", number);
    }
    
    /**
     * Formats a decimal number with specified precision.
     * 
     * @param number The number to format
     * @param precision The number of decimal places
     * @return Formatted decimal string
     */
    public static String formatDecimal(double number, int precision) {
        return String.format("%." + precision + "f", number);
    }
}
