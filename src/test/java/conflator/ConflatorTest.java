package conflator;

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

    Conflator<SequentialCharacterMessage> conflator;
    int currChar;

    @Test(timeout = 1000)
    public void should_put_and_take() throws InterruptedException {
        // test
        conflator.put(new SequentialCharacterMessage("1", "a"));
        Message message = conflator.take();

        // check
        assertNotNull(message);
        assertEquals(message.body(), "a");
    }

    @Test(timeout = 1000)
    public void should_merge_messages_on_same_key() throws InterruptedException {
        // given
        conflator.put(new SequentialCharacterMessage("1", "a"));
        conflator.put(new SequentialCharacterMessage("1", "b"));

        // test
        Thread.sleep(300); // waiting the conflation
        Message message = conflator.take();

        // check
        assertNotNull(message);
        assertEquals("ab", message.body());
        assertTrue(message.isMerged());
    }

    @Test(timeout = 1000)
    public void should_not_merge_messages_on_same_key_and_long_delay() throws InterruptedException {
        // given
        conflator.pause(500);
        conflator.put(new SequentialCharacterMessage("1", "a"));
        conflator.put(new SequentialCharacterMessage("1", "b"));

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

    @Test(timeout = 1000)
    public void should_put_and_take_on_two_keys() throws InterruptedException {
        // given
        conflator.put(new SequentialCharacterMessage("1", "a"));
        conflator.put(new SequentialCharacterMessage("2", "A"));

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
            System.out.println("size= " + conflator.size());
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

    @Test
    public void should_conflate_all_messages() {
        // given
        conflator.stop();
        for (int i = 0; i < 10_000; i++) {
            conflator.put(new SequentialCharacterMessage("key", generator()));
        }

        // test
        for (int i = 0; i < 10; i++) {
            conflator.conflate(1000);
        }
        SequentialCharacterMessage message = conflator.take();

        // check
        assertEquals(0, conflator.size());
        assertNotNull(message);
        assertEquals(message.body().length(), 10_000);
    }

    //// Methods for the tests

    private synchronized String generator() {
        String result = String.valueOf(currChar++);
        if (currChar > 9) currChar = 0;
        return result;
    }

    @Before
    public void setUp() {
        conflator = new MultiValuedMapConflator<>(true);
    }

    @After
    public void tearDown() {
        conflator.stop();
    }

    /**
     * Send messages provided by {@link #generator()}.
     */
    class Sender implements Callable<Boolean> {

        private Conflator<SequentialCharacterMessage> conflator;
        private String key;
        private int msgCount;


        @Override
        public Boolean call() {
            boolean result = false;
            for (int j = 0; j < msgCount; j++) {
                try {
                    conflator.put(new SequentialCharacterMessage(key, generator()));
                    result = true;
                } catch (Exception e) {
                    e.fillInStackTrace();
                }
            }
            return result;
        }

        Sender(Conflator<SequentialCharacterMessage> conflator, String key, int msgCount) {
            this.conflator = conflator;
            this.key = key;
            this.msgCount = msgCount;
        }
    }

}
