package cassandra.strategy;

import static org.junit.Assert.fail;

import java.lang.reflect.Method;

import org.apache.cassandra.db.compaction.TimeWindowCompactionStrategyHelper;
import org.junit.Test;

public class TestRef {

    @Test
    public void test() {

        try {
            Method mGetTables = TimeWindowCompactionStrategyHelper.class.getDeclaredMethod(//
                    "getNextBackgroundSSTables", int.class//
            );
            mGetTables.setAccessible(true);
        } catch (Exception e) {
            fail();

        }

    }
}
