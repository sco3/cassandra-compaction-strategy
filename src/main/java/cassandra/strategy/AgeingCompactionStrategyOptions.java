package cassandra.strategy;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AgeingCompactionStrategyOptions
        implements StrategyConstants {
    static Logger mTrace = LoggerFactory.getLogger(//
            AgeingCompactionStrategyOptions.class//
    );

    public static String AGE_MINUTES = "age_minutes";
    public static long DEFAULT_AGE_MINUTES = Long.MAX_VALUE;

    public static String MAX_AGED_TABLES = "max_aged_tables";
    public static int DEFAULT_MAX_AGED_TABLES = 32;

    public static String MAX_LDT = "max_local_deletion_time";
    public static int DEFAULT_MAX_LDT = 2147483647;

    public static String DRY_RUN = "dry_run";
    public static boolean DEFAULT_DRY_RUN = true;

    public long mAgeMinutes = DEFAULT_AGE_MINUTES;
    public int mAgedTables = DEFAULT_MAX_AGED_TABLES;
    public int mMaxLdt = DEFAULT_MAX_LDT;
    public boolean mDryRun = DEFAULT_DRY_RUN;

    public long mAgeMicros = 0;

    public static long DEFAULT_MAX_FILE_SILE = Long.MAX_VALUE;
    public long mMaxFileSize = DEFAULT_MAX_FILE_SILE;
    public static String MAX_FILE_SILE_MB = "max_file_size_mb";

    public AgeingCompactionStrategyOptions(Map<String, String> options) {
        {
            String optionValue = options.get(AGE_MINUTES);
            mAgeMinutes = optionValue == null ? //
                    DEFAULT_AGE_MINUTES : Long.parseLong(optionValue);

            mAgeMicros = MICROSECONDS.convert(mAgeMinutes, MINUTES);
        }
        {
            String optionValue = options.get(MAX_AGED_TABLES);
            mAgedTables = optionValue == null ? //
                    DEFAULT_MAX_AGED_TABLES : Integer.parseInt(optionValue);
        }
        {
            String optionValue = options.get(MAX_LDT);
            mMaxLdt = optionValue == null ? //
                    DEFAULT_MAX_LDT : Integer.parseInt(optionValue);
        }
        {
            String optionValue = options.get(DRY_RUN);
            mDryRun = optionValue == null ? //
                    DEFAULT_DRY_RUN : Boolean.parseBoolean(optionValue);
        }
        {
            String optionValue = options.get(MAX_FILE_SILE_MB);
            mMaxFileSize = optionValue == null ? DEFAULT_MAX_FILE_SILE
                    : 1024 * 1024 * Long.parseLong(optionValue);
        }
    }

}
