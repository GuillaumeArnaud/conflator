package benchmark;

import conflator.Conflator;
import conflator.Message;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class ConflatorBenchmark {
    Conflator conflator;

    @GenerateMicroBenchmark
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

    }

    @Setup
    public void setUp() {
        conflator = new Conflator();
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

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + ConflatorBenchmark.class.getSimpleName() + ".*")
                .warmupIterations(5)
                .measurementIterations(5)
                .forks(1)
                .verbosity(VerboseMode.EXTRA)
                .build();

        new Runner(opt).run();
    }

}
