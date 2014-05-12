package benchmark;

import freelock.LockFreeQueue;
import freelock.SynchronizedQueue;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class LockFreeBenchmark {
    @State(Scope.Benchmark)
    public static class SharedCounters {
        @Param(value = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "r", "s", "t", "u", "v", "w", "x", "y", "z", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "r", "s", "t", "u", "v", "w", "x", "y", "z"})
        private String message="a";

        private static LockFreeQueue<String> queue = null;
        private static SynchronizedQueue<String> queueSync = null;
        private static ConcurrentLinkedQueue<String> queueCLQ = null;
    }

    @GenerateMicroBenchmark
    @OperationsPerInvocation(5_000)
    public void lock_free() {
        for (int i = 0; i < 5_000; i++) {
            String response = SharedCounters.queue.pick();
            if (response == null)
                throw new IllegalArgumentException();
        }
    }

    @GenerateMicroBenchmark
    @OperationsPerInvocation(5_000)
    public void synchronized_queue() {
        for (int i = 0; i < 5_000; i++) {
            String response = SharedCounters.queueSync.pick();
            if (response == null)
                throw new IllegalArgumentException();
        }
    }

    @GenerateMicroBenchmark
    @OperationsPerInvocation(5_000)
    public void concurrent_linked_queue() {
        for (int i = 0; i < 5_000; i++) {
            String response = SharedCounters.queueSync.pick();
            if (response == null)
                throw new IllegalArgumentException();
        }
    }

    SharedCounters sharedCounters = new SharedCounters();

    @Setup
    public void setUp() {
        System.out.println("setup " + sharedCounters.message);
        SharedCounters.queue = new LockFreeQueue<>();
        SharedCounters.queueSync = new SynchronizedQueue<>();
        SharedCounters.queueCLQ = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < 5_000_000; i++) {
            SharedCounters.queue.put(sharedCounters.message);
            SharedCounters.queueSync.put(sharedCounters.message);
            SharedCounters.queueCLQ.offer(sharedCounters.message);
        }
    }

    @TearDown
    public void tearDown() {
        System.out.println("tear down");
    }

    public static void main(String[] args) throws RunnerException {
        System.out.println(".*" + LockFreeBenchmark.class.getSimpleName() + ".*");
        Options opt = new OptionsBuilder()
                .include(".*" + LockFreeBenchmark.class.getSimpleName() + ".*")
                .warmupIterations(5)
                .measurementIterations(45)
                .forks(2)
                .threads(20)
                .verbosity(VerboseMode.EXTRA)
                        //.addProfiler(ProfilerType.GC)
                .jvmArgs("-Xmx512m")
                .build();

        new Runner(opt).run();
    }
}
