package conflator;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class ConflatorTest {

    Conflator conflator;
    int currChar;

    @Test
    public void should_put_and_take() throws InterruptedException {
        // test
        conflator.put(new Message("1", "a"));
        Message message = conflator.take();

        // check
        assertNotNull(message);
        assertEquals(message.body(), "a");
    }

    @Test
    public void should_merge_messages_on_same_key() throws InterruptedException {
        // given
        conflator.put(new Message("1", "a"));
        conflator.put(new Message("1", "b"));

        // test
        Thread.sleep(300); // waiting the conflation
        Message message = conflator.take();

        // check
        assertNotNull(message);
        assertEquals("ab", message.body());
        assertTrue(message.isMerged());
    }

    @Test
    public void should_not_merge_messages_on_same_key_and_long_delay() throws InterruptedException {
        // given
        conflator.pause(500);
        conflator.put(new Message("1", "a"));
        conflator.put(new Message("1", "b"));

        // test
        Message message1 = conflator.take();
        Message message2 = conflator.take();

        // check
        assertNotNull(message1);
        assertEquals(message1.body(), "a");
        assertEquals(message1.isMerged(), false);

        assertNotNull(message2);
        assertEquals(message2.body(), "b");
        assertEquals(message2.isMerged(), false);
    }

    @Test
    public void should_put_and_take_on_two_keys() throws InterruptedException {
        // given
        conflator.put(new Message("1", "a"));
        conflator.put(new Message("2", "A"));

        // test
        Message message1 = conflator.take();
        Message message2 = conflator.take();

        // check
        assertNotNull(message1);
        assertEquals(message1.body(), "a");
        assertNotNull(message2);
        assertEquals(message2.body(), "A");
    }

    @Test
    public void two_unmergeable_messages_should_be_remain_unmergeable() {
        Message message1 = new Message("key", "a");
        Message message2 = new Message("key", "b");
        Message message3 = new Message("key", "d");
        Message message4 = new Message("key", "e");

        List<Message> messages = conflator.merge(Lists.newArrayList(message1, message2, message3, message4));

        assertNotNull(messages);
        assertEquals(messages.size(), 3);
        assertEquals(messages.get(0).body(), "ab");
        assertEquals(messages.get(1).body(), "d");
        assertEquals(messages.get(2).body(), "e");
    }

    @Test
    public void two_mergeable_messages_should_be_merged() {
        Message message1 = new Message("key", "a");
        Message message2 = new Message("key", "b");

        List<Message> messages = conflator.merge(Lists.newArrayList(message1, message2));

        assertNotNull(messages);
        assertEquals(messages.size(), 1);
        assertEquals(messages.get(0).body(), "ab");

    }

    @Test
    public void should_receive_100_000_msgs_sent_on_10_threads() throws ExecutionException, InterruptedException {
        // given
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        List<Future<Boolean>> senders = new ArrayList<>();
        int totalMsgCount = 10_000;
        int differentKeyCount = 10;

        // test
        for (int i = 0; i < differentKeyCount; i++) {
            senders.add(executorService.submit(new Sender(conflator, "key" + i, totalMsgCount)));
        }

        // wait for that all messages are sent
        for (Future future : senders) {
            future.get();
        }

        // asserts on received messages
        long counter = 0, mergeCounter = 0;
        Map<String, Long> counters = new HashMap<>();
        for (int i = 0; i < differentKeyCount; i++) {
            counters.put("key" + i, 0l);
        }
        while (counter < totalMsgCount * differentKeyCount) {
            Message message = conflator.take();
            mergeCounter += message.mergesCount();
            long oldCount = counters.get(message.key());
            counter += message.mergesCount() + 1;
            long newCount = oldCount + message.mergesCount() + 1;
            counters.put(message.key(), newCount);
            assertTrue(message.isValid());
        }
        for (String key : counters.keySet()) {
            assertTrue(totalMsgCount == counters.get(key));
        }
        assertTrue(mergeCounter > 0);
        System.out.println("merges : " + mergeCounter);
    }

    //// Methods for the tests

    private synchronized String generator() {
        String result = String.valueOf(currChar++);
        if (currChar > 9) currChar = 0;
        return result;
    }


    @Before
    public void setUp() {
        conflator = new Conflator();
    }

    @After
    public void tearDown() {
        conflator.stop();
    }

    /**
     * Send messages provided by {@link #generator()}.
     */
    class Sender implements Callable<Boolean> {

        private Conflator conflator;
        private String key;
        private int msgCount;


        @Override
        public Boolean call() {
            boolean result = false;
            for (int j = 0; j < msgCount; j++) {
                try {
                    conflator.put(new Message(key, generator()));
                    result = true;
                } catch (Exception e) {
                    e.fillInStackTrace();
                }
            }
            return result;
        }

        Sender(Conflator conflator, String key, int msgCount) {
            this.conflator = conflator;
            this.key = key;
            this.msgCount = msgCount;
        }
    }

}
