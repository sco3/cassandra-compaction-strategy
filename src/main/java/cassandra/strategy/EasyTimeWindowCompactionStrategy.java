package cassandra.strategy;

import static cassandra.strategy.EasyTimeWindowCompactionStrategyOptions.MAX_FILE_SILE_MB;
import static com.google.common.collect.Iterables.filter;
import static org.apache.commons.lang3.builder.ToStringBuilder.reflectionToString;
//import static org.apache.commons.lang3.builder.ToStringStyle.MULTI_LINE_STYLE;
import static org.apache.commons.lang3.builder.ToStringStyle.SHORT_PREFIX_STYLE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.TimeWindowCompactionStrategyHelper;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class EasyTimeWindowCompactionStrategy
        extends TimeWindowCompactionStrategyHelper //
        implements StrategyConstants {

    static Logger mTrace = LoggerFactory.getLogger(//
            EasyTimeWindowCompactionStrategy.class//
    );

    EasyTimeWindowCompactionStrategyOptions mOptions;

    public EasyTimeWindowCompactionStrategy(//
            ColumnFamilyStore cfs, //
            Map<String, String> props//
    ) {
        super(cfs, removeOptions(props));

        mOptions = new EasyTimeWindowCompactionStrategyOptions(props);
        mTrace.info("Options: {} {} {} {}", //
                props, //
                reflectionToString(mOptions, SHORT_PREFIX_STYLE), //
                reflectionToString(options, SHORT_PREFIX_STYLE),
                reflectionToString(options.stcsOptions, SHORT_PREFIX_STYLE)//
        );
    }

    public static Map<String, String> removeOptions( //
            Map<String, String> options//
    ) throws ConfigurationException {
        HashMap<String, String> map = new HashMap<>(options);
        map.remove(MAX_FILE_SILE_MB);
        return map;
    }

    public static Map<String, String> validateOptions(//
            Map<String, String> options//
    ) {
        return TimeWindowCompactionStrategyHelper.validateOptions(//
                removeOptions(options)//
        );
    }

    @Override
    public synchronized List<SSTableReader> getNextBackgroundSSTables(
            final int gcBefore) {
        mTrace.debug("Get sstables");
        Iterable<SSTableReader> liveTables = cfs.getSSTables(SSTableSet.LIVE);
        if (Iterables.isEmpty(liveTables))
            return Collections.emptyList();

        Set<SSTableReader> uncompacting = ImmutableSet
                .copyOf(filter(cfs.getUncompactingSSTables(), sstables::contains));

        // Find fully expired SSTables. Those will be included no matter what.
        Set<SSTableReader> expired = Collections.emptySet();

        if (System.currentTimeMillis()
                - lastExpiredCheck > options.expiredSSTableCheckFrequency) {
            mTrace.debug(
                    "TWCS expired check sufficiently far in the past, checking for fully expired SSTables");
            mTrace.info("Checking droppable sstables in {}", cfs);
            expired = getFullyExpiredSSTables(uncompacting, gcBefore);
            lastExpiredCheck = System.currentTimeMillis();
        } else {
            mTrace.debug("TWCS skipping check for fully expired SSTables");
        }

        Set<SSTableReader> candidates = Sets
                .newHashSet(filterSuspectSSTables(uncompacting));

        List<SSTableReader> compactionCandidates = new ArrayList<>(
                getNextNonExpiredSSTables(Sets.difference(candidates, expired),
                        gcBefore));

        // do not compact big files
        ListIterator<SSTableReader> iter = compactionCandidates.listIterator();
        while (iter.hasNext()) {
            SSTableReader reader = iter.next();
            if (reader.bytesOnDisk() > mOptions.mMaxFileSize) {
                mTrace.debug("Skip big file: {}", reader);
                iter.remove();
            }
        }

        if (!expired.isEmpty()) {
            mTrace.debug("Including expired sstables: {}", expired);
            compactionCandidates.addAll(expired);
        }
        return compactionCandidates;
    }

    protected Set<SSTableReader> getFullyExpiredSSTables(//
            Iterable<SSTableReader> compacting, //
            int gcBefore//
    ) {
        Set<SSTableReader> fullyExpired = new HashSet<>();
        for (SSTableReader candidate : compacting) {
            if (candidate.getSSTableMetadata().maxLocalDeletionTime < gcBefore) {
                fullyExpired.add(candidate);
                mTrace.info(
                        "Dropping expired SSTable {} (maxLocalDeletionTime={}, gcBefore={})",
                        candidate, candidate.getSSTableMetadata().maxLocalDeletionTime,
                        gcBefore);
            }
        }
        return fullyExpired;
    }
}