package conflator;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class TradeMergeTest {
    private MultiValuedMapConflator conflator;

    @Test
    public void two_unmergeable_trades_should_be_remain_unmergeable() {
        Trade trade1 = new Trade("GOOG", 100L);
        Trade trade2 = new Trade("AAPL", 120L);

        List<Trade> messages = conflator.merge(Lists.newArrayList(trade1, trade2));

        assertNotNull(messages);
        assertEquals(messages.size(), 2);
        assertEquals(Long.valueOf(messages.get(0).body()).longValue(), trade1.getCurrentQuantity());
        assertEquals(Long.valueOf(messages.get(1).body()).longValue(), trade2.getCurrentQuantity());
    }

    @Test(timeout = 1000)
    public void two_mergeable_trades_should_be_merged() {
        Trade trade1 = new Trade("GOOG", 100L);
        Trade Trade2 = new Trade("GOOG", 120L);

        List<Trade> messages = conflator.merge(Lists.newArrayList(trade1, Trade2));

        assertNotNull(messages);
        assertEquals(messages.size(), 1);
        assertEquals(Long.valueOf(messages.get(0).body()).longValue(), trade1.getInitialQuantity() + Trade2.getInitialQuantity());
    }

    @Test(timeout = 1000)
    public void should_merge_messages_on_same_key() throws InterruptedException {
        // given
        conflator.put(new Trade("GOOG", 100L));
        conflator.put(new Trade("GOOG", 120L));

        // test
        Thread.sleep(300); // waiting the conflation
        Message message = conflator.take();

        // check
        assertNotNull(message);
        assertEquals(Long.valueOf(message.body()).longValue(), 220L);
        assertTrue(message.isMerged());
    }

    @Test(timeout = 1000)
    public void should_not_merge_messages_on_diff_key() throws InterruptedException {
        // given
        conflator.put(new Trade("GOOG", 100L));
        conflator.put(new Trade("AAPL", 120L));

        // test
        Thread.sleep(300); // waiting the conflation
        Message message1 = conflator.take();
        Message message2 = conflator.take();

        // check
        assertNotNull(message1);
        assertNotNull(message2);

        assertEquals(Long.valueOf(message1.body()).longValue(), 100L);
        assertFalse(message1.isMerged());

        assertEquals(Long.valueOf(message2.body()).longValue(), 120L);
        assertFalse(message2.isMerged());
    }

    @Before
    public void setUp() {
        conflator = new MultiValuedMapConflator<Trade>(true);
    }

    @After
    public void tearDown() {
        conflator.stop();
    }
}
