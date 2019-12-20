package cassandra.strategy;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EasyTimeWindowCompactionStrategyOptions implements StrategyConstants {
    static Logger mTrace = LoggerFactory.getLogger(//
            EasyTimeWindowCompactionStrategyOptions.class//
    );
    public static long DEFAULT_MAX_FILE_SILE = Long.MAX_VALUE;
    public long mMaxFileSize = DEFAULT_MAX_FILE_SILE;
    public static String MAX_FILE_SILE_MB = "max_file_size_mb";

    public EasyTimeWindowCompactionStrategyOptions(Map<String, String> options) {
        String optionValue = options.get(MAX_FILE_SILE_MB);
        mMaxFileSize = optionValue == null ? DEFAULT_MAX_FILE_SILE
                : 1024 * 1024 * Long.parseLong(optionValue);
    }
}
