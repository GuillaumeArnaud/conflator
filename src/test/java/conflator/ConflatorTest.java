package conflator;

import com.google.common.base.Joiner;
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class ConflatorTest {

    Conflator conflator;

    @Test
    public void should_put_and_take() throws InterruptedException {
        // test
        conflator.put(new Message("1", "a"));
        Message message = conflator.take();

        // check
        assertThat(message, notNullValue());
        assertThat(message.body(), equalTo("a"));
    }

    @Test
    public void should_merge_messages_on_same_key() throws InterruptedException {
        // given
        conflator.put(new Message("1", "a"));
        conflator.put(new Message("1", "b"));

        // test
        Thread.sleep(100); // waiting the conflation
        Message message = conflator.take();

        // check
        assertThat(message, notNullValue());
        assertThat(message.body(), equalTo("ab"));
        assertThat(message.merged(), equalTo(true));
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
        assertThat(message1, notNullValue());
        assertThat(message1.body(), equalTo("a"));
        assertThat(message1.merged(), equalTo(false));

        assertThat(message2, notNullValue());
        assertThat(message2.body(), equalTo("b"));
        assertThat(message2.merged(), equalTo(false));
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
        assertThat(message1, notNullValue());
        assertThat(message1.body(), equalTo("a"));
        assertThat(message2, notNullValue());
        assertThat(message2.body(), equalTo("A"));
    }

    @Test
    public void should_receive_100_000_msgs_sent_on_10_threads() throws ExecutionException, InterruptedException {
        // given
        ExecutorService executorService = Executors.newFixedThreadPool(11);
        List<Future<Boolean>> senders = new ArrayList<>();

        // test
        Future<Map<String, Integer>> receiver = executorService.submit(new Receiver(conflator, 10 * 10_000));
        for (int i = 0; i < 10; i++) {
            senders.add(executorService.submit(new Sender(conflator, "key" + i, 10_000)));
        }
        for (Future future : senders) {
            future.get();
        }
        // wait the receiver
        Map<String, Integer> wordsByCount = receiver.get();

        System.out.println("Received messages by key:\n" + Joiner.on("\n").withKeyValueSeparator("-").join(wordsByCount));
        for (Integer value : wordsByCount.values()) {
            assertEquals(Integer.valueOf(10000), value);
        }
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
     * Send sequentially the messages '0123456789', letter by letter (so 10 messages).
     */
    class Sender implements Callable<Boolean> {

        private Conflator conflator;
        private String key;
        private int wordTotal;


        Sender(Conflator conflator, String key, int wordTotal) {
            this.conflator = conflator;
            this.key = key;
            this.wordTotal = wordTotal;
        }

        @Override
        public Boolean call() throws Exception {
            for (int j = 0; j < wordTotal; j++)
                // send the message '0123456789' letter by letter
                for (int i = 0; i < 10; i++)
                    conflator.put(new Message(key, String.valueOf(i)));
            return true;
        }
    }

    /**
     * Receive messages from conflator and return a counter by key
     */
    class Receiver implements Callable<Map<String, Integer>> {

        private Conflator conflator;
        /**
         * Total number of expected words
         */
        private int wordTotal;

        Receiver(Conflator conflator, int wordTotal) {
            this.conflator = conflator;
            this.wordTotal = wordTotal;
        }


        @Override
        public Map<String, Integer> call() throws Exception {
            Map<String, Integer> wordsByKey = new HashMap<>();
            Map<String, String> msgByKey = new HashMap<>();
            int total = 0;
            while (total < wordTotal) {

                // take last message in conflator
                Message msg = conflator.take();
                String keyMsg = msg.key(); // the concerned key

                // init current store map for the key
                if (!msgByKey.containsKey(keyMsg)) msgByKey.put(keyMsg, "");

                // add new body to the current store map for the given key
                msgByKey.put(keyMsg, msgByKey.get(keyMsg) + msg.body());

                String body = msgByKey.get(keyMsg);
                while (body.length() >= 10) {
                    // the current store word contains the searched word '0123456789'

                    // check that contains the right word
                    String word = body.substring(0, 10);
                    assertEquals("0123456789", word);

                    // it's ok so let's increase counters
                    if (!wordsByKey.containsKey(keyMsg)) wordsByKey.put(keyMsg, 1);
                    else wordsByKey.put(keyMsg, wordsByKey.get(keyMsg) + 1);

                    // update the current word by removing the starting word
                    String newBody = body.substring(10, body.length());
                    msgByKey.put(keyMsg, newBody);
                    body = msgByKey.get(keyMsg);
                }
                total = 0;
                for (Integer c : wordsByKey.values()) {
                    total += c;
                }
            }
            return wordsByKey;
        }
    }
}
