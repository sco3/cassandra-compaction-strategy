package cassandra.strategy;

import static cassandra.strategy.AgeingCompactionStrategyOptions.DEFAULT_AGE_MINUTES;
import static cassandra.strategy.AgeingCompactionStrategyOptions.DEFAULT_MAX_FILE_SILE;
import static cassandra.strategy.AgeingCompactionStrategyOptions.DRY_RUN;
import static cassandra.strategy.AgeingCompactionStrategyOptions.AGE_MINUTES;
import static cassandra.strategy.AgeingCompactionStrategyOptions.MAX_AGED_TABLES;
import static cassandra.strategy.AgeingCompactionStrategyOptions.MAX_LDT;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.builder.ToStringBuilder.reflectionToString;
import static org.apache.commons.lang3.builder.ToStringStyle.SHORT_PREFIX_STYLE;
//import static org.apache.commons.lang3.builder.ToStringStyle.MULTI_LINE_STYLE;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.TimeWindowCompactionStrategyHelper;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AgeingCompactionStrategy
        extends TimeWindowCompactionStrategyHelper //
        implements StrategyConstants {
    static Logger mTrace = LoggerFactory.getLogger(//
            AgeingCompactionStrategy.class//
    );

    private AgeingCompactionStrategyOptions mOptions;
    private long mLastDropMillis = 0;

    private Comparator<SSTableReader> mTsComparator;
    private SimpleDateFormat mDtFmt;
    private long mGcBeforeMicros = 0;

    private int mGcBefore;

    public static Map<String, String> removeOptions( //
            Map<String, String> options//
    ) throws ConfigurationException {
        HashMap<String, String> map = new HashMap<>(options);
        map.remove(AGE_MINUTES);
        map.remove(MAX_AGED_TABLES);
        map.remove(MAX_LDT);
        map.remove(DRY_RUN);
        return map;
    }

    public AgeingCompactionStrategy(//
            ColumnFamilyStore cfs, //
            Map<String, String> props//
    ) {
        super(cfs, removeOptions(props));
        mOptions = new AgeingCompactionStrategyOptions(props);
        mTrace.info("{} options: {} {} {} {}", //
                cfs.name, //
                props, //
                bean(mOptions), //
                bean(options), //
                bean(options.stcsOptions)//
        );
        mTsComparator = new Comparator<SSTableReader>() {
            @Override
            public int compare(SSTableReader o1, SSTableReader o2) {
                long ts1 = o1.getSSTableMetadata().maxTimestamp;
                long ts2 = o2.getSSTableMetadata().maxTimestamp;
                if (ts1 > ts2) {
                    return 1;
                } else if (ts2 > ts1) {
                    return -1;
                } else {
                    return 0;
                }
            }
        };
        mDtFmt = new SimpleDateFormat("yyyyMMdd.HHmm");
        mDtFmt.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private String bean(Object options) {
        return reflectionToString(options, SHORT_PREFIX_STYLE);
    }

    public static Map<String, String> validateOptions(//
            Map<String, String> options//
    ) {
        return TimeWindowCompactionStrategyHelper.validateOptions(//
                removeOptions(options)//
        );
    }

    String fmtMicros(long d) {
        long millis = MILLISECONDS.convert(d, MICROSECONDS);
        return mDtFmt.format(new Date(millis));
    }

    String fmtSeconds(long d) {
        long millis = MILLISECONDS.convert(d, SECONDS);
        return mDtFmt.format(new Date(millis));
    }

    @Override
    public synchronized List<SSTableReader> getNextBackgroundSSTables(
            final int gcBefore) {

        long gcBeforeMicros = TimeUnit.MICROSECONDS.convert(//
                gcBefore, TimeUnit.SECONDS//
        );
        if (gcBeforeMicros > mGcBeforeMicros) {
            mGcBeforeMicros = gcBeforeMicros;
            mGcBefore = gcBefore;
        }
        // mTrace.debug("" //
        // + "Get sstables. GC before: {} {} options: {} {} {}", //
        // fmtMicros(gcBeforeMicros), //
        // cfs.name, //
        // bean(mOptions), //
        // bean(options), //
        // bean(options.stcsOptions)//
        // );

        List<SSTableReader> result = null;
        if (currentTimeMillis() - mLastDropMillis > //
        options.expiredSSTableCheckFrequency//
        ) {
            if (mOptions.mAgeMinutes < DEFAULT_AGE_MINUTES) {
                Iterable<SSTableReader> liveTables = cfs.getLiveSSTables();
                mTrace.debug("" //
                        + "{} Check expired sstables " //
                        + "(max_ts + expire_minutes({}) < {})", //
                        cfs.name, //
                        mOptions.mAgeMinutes, //
                        fmtMicros(gcBeforeMicros)//
                );
                result = new ArrayList<>();
                for (SSTableReader table : liveTables) {

                    StatsMetadata meta = table.getSSTableMetadata();
                    long maxTs = meta.maxTimestamp;
                    int maxLdt = meta.maxLocalDeletionTime;
                    long expires = maxTs + mOptions.mAgeMicros;
                    mTrace.debug("" //
                            + "Check {} expires: {} maxTs: {} " //
                            + "minTTL: {} maxTTL: {} maxLDT: {}", //
                            table.getFilename(), //
                            fmtMicros(expires), //
                            fmtMicros(table.getSSTableMetadata().maxTimestamp), //
                            table.getSSTableMetadata().minTTL, //
                            table.getSSTableMetadata().maxTTL, //
                            fmtSeconds(table.getSSTableMetadata().maxLocalDeletionTime)
                    //
                    );

                    if (expires < gcBeforeMicros //
                            && maxLdt >= mOptions.mMaxLdt) {

                        if (mOptions.mDryRun == false) {
                            result.add(table);
                        }

                        mTrace.debug("" //
                                + "Remove {} {} expires:{} maxTs: {} " //
                                + "minTTL: {} maxTTL: {} maxLDT: {} ", //
                                mOptions.mDryRun ? "(dry-run)" : "", //
                                table.getFilename(), //
                                fmtMicros(expires), //
                                fmtMicros(meta.maxTimestamp), //
                                meta.minTTL, //
                                meta.maxTTL, //
                                fmtSeconds(meta.maxLocalDeletionTime) //
                        );
                    }
                }

                int all = result.size();
                if (all > 0) {
                    Collections.sort(result, mTsComparator);
                    if (all > mOptions.mAgedTables) {
                        result.subList(mOptions.mAgedTables, all).clear();
                    }
                    long microBegin = result.get(0)//
                            .getSSTableMetadata().maxTimestamp;
                    long microEnd = result.get(result.size() - 1) //
                            .getSSTableMetadata().maxTimestamp;

                    mTrace.debug("" //
                            + "Expired sstables: {}/{} {} - {}", //
                            result.size(), all, //
                            fmtMicros(microBegin), //
                            fmtMicros(microEnd) //
                    );
                }
            }
            mLastDropMillis = System.currentTimeMillis();
        }
        List<SSTableReader> tables = super.getNextBackgroundSSTables(gcBefore);
        // mTrace.debug("Super class tables: {}", tables.size());
        if (result == null) {
            result = tables;

            if (mOptions.mMaxFileSize < DEFAULT_MAX_FILE_SILE) {
                // do not compact big files
                ListIterator<SSTableReader> iter = result.listIterator();
                while (iter.hasNext()) {
                    SSTableReader reader = iter.next();
                    if (reader.bytesOnDisk() > mOptions.mMaxFileSize) {
                        mTrace.debug("" //
                                + "Skip big file: {} bytes: {} > {}", //
                                reader, reader.bytesOnDisk(), mOptions.mMaxFileSize//
                        );
                        iter.remove();
                    }
                }
            }

        } else {
            for (SSTableReader table : tables) {
                long maxTs = table.getSSTableMetadata().maxTimestamp;
                int ldt = table.getSSTableMetadata().maxLocalDeletionTime;
                if (maxTs + mOptions.mAgeMicros < gcBeforeMicros //
                        || ldt < gcBefore) {
                    // mTrace.debug(//
                    // "Add {} maxTs: {} minTTL: {} maxTTL: {} maxLDT: {}", //
                    // table.getFilename(), //
                    // fmtMicros(table.getSSTableMetadata().maxTimestamp), //
                    // table.getSSTableMetadata().minTTL, //
                    // table.getSSTableMetadata().maxTTL, //
                    // fmtSeconds(ldt) //
                    // );
                    if (false == result.contains(table)) {
                        result.add(table);
                    }
                }
            }
        }
        return result;
    }

    @Override
    // @SuppressWarnings("resource")
    public ScannerList getScanners(Collection<SSTableReader> sstables,
            Collection<Range<Token>> ranges) {

        Collection<SSTableReader> list = sstables;
        if (mOptions.mDryRun == false //
                && mOptions.mAgeMinutes < DEFAULT_AGE_MINUTES //
                && mGcBeforeMicros > 0//
        ) {
            // mTrace.debug("" //
            // + "Get scanners GC before: {} {} options: {} {} {}", //
            // fmtMicros(mGcBeforeMicros), //
            // cfs.name, //
            // bean(mOptions), //
            // bean(options), //
            // bean(options.stcsOptions)//
            // );

            list = new ArrayList<>();
            int drops = 0;
            for (SSTableReader table : sstables) {
                long maxTs = table.getSSTableMetadata().maxTimestamp;
                int ldt = table.getSSTableMetadata().maxLocalDeletionTime;
                if (maxTs + mOptions.mAgeMicros < mGcBeforeMicros //
                        || ldt < mGcBefore) {
                    drops++;
                } else {
                    list.add(table);
                }
            }

            mTrace.debug("" //
                    + "{} Scanners: {} drops: {} compact: {} GC before: {}", //
                    cfs.name, //
                    sstables.size(), drops, //
                    list.size(), fmtMicros(mGcBeforeMicros)//
            );
        }

        return super.getScanners(list, ranges);
    }

}