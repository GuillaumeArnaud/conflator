package benchmark;

import freelock.LockFreeQueue;
import freelock.SynchronizedQueue;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class SynchronizedQueueBenchmark {
    @State(Scope.Benchmark)
    public static class SharedCounters {
        private static SynchronizedQueue<String> queue = null;
    }

    @GenerateMicroBenchmark
    public void lock_free() {
        for (int i = 0; i < 10_000; i++) {
            String response = SharedCounters.queue.pick();
            if (response == null)
                throw new IllegalArgumentException();
        }
    }

    @Setup
    public void setUp() {
        System.out.println("setup");
        SharedCounters.queue = new SynchronizedQueue<>();
        for (int i = 0; i < 10_000_000; i++)
            SharedCounters.queue.put("a");
    }

    @TearDown
    public void tearDown() {
        System.out.println("tear down");
    }

    public static void main(String[] args) throws RunnerException {
        System.out.println(".*" + SynchronizedQueueBenchmark.class.getSimpleName() + ".*");
        Options opt = new OptionsBuilder()
                .include(".*" + SynchronizedQueueBenchmark.class.getSimpleName() + ".*")
                .warmupIterations(0)
                .measurementIterations(50)
                .forks(1)
                .threads(20)
                .verbosity(VerboseMode.EXTRA)
                .jvmArgs("-Xmx512m")
                .build();

        new Runner(opt).run();
    }}
