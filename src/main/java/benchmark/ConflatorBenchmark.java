package benchmark;

import conflator.Conflator;
import conflator.MultiValuedMapConflator;
import conflator.SequentialCharacterMessage;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class ConflatorBenchmark {
    Conflator<SequentialCharacterMessage> conflator;
    int currChar;
    LinkedBlockingQueue<SequentialCharacterMessage> queue;

    @GenerateMicroBenchmark
    public void put_and_take_100_000_msgs() {
        conflator.daemonize();
        for (int i = 0; i < 100_000; i++) {
            conflator.put(new SequentialCharacterMessage("key", generator()));
            SequentialCharacterMessage message = conflator.take();
            assert message != null;
            assert message.isValid();
        }
    }

    @GenerateMicroBenchmark
    public void put_and_take_100_000_msgs_on_blocking_queue() throws InterruptedException {
        conflator.daemonize();
        for (int i = 0; i < 100_000; i++) {
            queue.put(new SequentialCharacterMessage("key", generator()));
            SequentialCharacterMessage message = queue.take();
            assert message != null;
            assert message.isValid();
        }
    }

    private synchronized String generator() {
        String result = String.valueOf(currChar++);
        if (currChar > 9) currChar = 0;
        return result;
    }

    @Setup
    public void setUp() {
        // simple queue to compare
        queue = new LinkedBlockingQueue<>();

        // create conflator but with the daemonized conflation
        conflator = new MultiValuedMapConflator<>(false);

        // Pre-fill of the conflator
        for (int i = 0; i < 100_00; i++) {
            SequentialCharacterMessage message = new SequentialCharacterMessage("key", generator());
            conflator.put(message);
            queue.add(message);
        }

    }

    public static void main(String[] args) throws RunnerException {
        System.out.println(".*" + ConflatorBenchmark.class.getSimpleName() + ".*");
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
