package conflator;

import com.google.common.collect.ArrayListMultimap;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.Thread.sleep;


public class MultiValuedMapConflator<M extends Message<M>> implements Conflator<M> {

    /**
     * The multivalued map which stores all messages indexed by key.
     */
    private ArrayListMultimap<String, M> data = ArrayListMultimap.create();

    /**
     * The queue which store the keys of the new messages. The queue, at an instant, must contain only one version of
     * the key.
     */
    private LinkedBlockingQueue<String> cursors = new LinkedBlockingQueue<>();

    /**
     * The blocking queue which receives message from external and before the dispatch to {@link #data}.
     */
    private BlockingQueue<M> queue = new LinkedBlockingQueue<>();

    /**
     * Lock for accessing to {@link #data}.
     */
    private Lock lock = new ReentrantLock();

    /**
     * Define if the conflation is daemonized in a background thread or not.
     */
    private volatile boolean daemonized;

    /**
     * Pause between each conflation. It's for test purpose only.
     */
    private long pauseInMs = 0;

    public MultiValuedMapConflator(boolean daemonized) {
        if (daemonized)
            daemonize();
    }

    @Override
    public void daemonize() {
        daemonized = true;
        // Start a thread for taking message on queue and put it in data concurrently to user action
        new Thread(() -> {
            while (daemonized) {
                try {
                    conflate();
                    sleep(pauseInMs);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            daemonized = false;
        }).start();
    }

    /**
     * Take one message from input queue and insert it into the data map.
     */
    private void conflate() {
        try {
            M receivedMessage = queue.take();
            // lock for updating data
            lock.lock();
            List<M> messagesForAKey = data.get(receivedMessage.key());
            int size = messagesForAKey.size();
            if (size == 0) {
                // there is no message currently for this key, so we can update the cursor
                cursors.put(receivedMessage.key());
            }
            // add the message to data
            messagesForAKey.add(receivedMessage);
            lock.unlock();
            // end of data updating
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        }
    }

    @Override
    public void conflate(long messageCount) {
        if (!daemonized)
            for (int i = 0; i < messageCount; i++) {
                conflate();
            }
    }

    @Override
    public void put(M message) {
        try {
            queue.put(message);
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        }

    }


    @Override
    public M take() {
        M message = null; // the returned message
        try {
            // take the new key of the next message
            String key = cursors.take();
            int size = 0; // number of message retrieving from {@link #data}
            while (size == 0) {
                lock.lock();
                List<M> messagesInData = data.removeAll(key); // remove messages from {@link #data}
                size = messagesInData.size(); // WARN the size could be 0 if the {@link #cursor} is updated before the update of {@link #data}. Should be very rare.
                if (size == 0) continue;
                List<M> afterMergeMessages = merge(messagesInData);
                message = afterMergeMessages.remove(0); // only the first one is a merged message
                if (afterMergeMessages.size() > 0) {
                    // some unmerged messages remain so they are put again in data and queue
                    data.putAll(key, afterMergeMessages);
                    cursors.put(key);
                }
                lock.unlock();
            }
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        }
        if (message == null) throw new IllegalStateException("Can't return a null message");
        return message;
    }

    /**
     * Merge the n-th first mergeable elements and the ordered list of messages for the current key.
     *
     * @param elements list of {@link SequentialCharacterMessage} to merge
     * @return the list of {@link SequentialCharacterMessage}. The
     */
    protected List<M> merge(List<M> elements) {
        List<M> messages = new ArrayList<>();

        M first = null;
        boolean skip = false;
        for (M message : elements) {
            if (first == null) {
                first = message;
                messages.add(message);
            } else if (!skip && !first.merge(message)) {
                messages.add(message);
                skip = true;
            } else if (skip) {
                messages.add(message);
            }
        }
        return messages;
    }

    @Override
    public void stop() {
        this.daemonized = false;
    }

    @Override
    public void pause(long pauseInMs) {
        this.pauseInMs = pauseInMs;
    }

    @Override
    public long size() {
        return queue.size() + data.size();
    }

    @Override
    public boolean isDaemonized() {
        return daemonized;
    }
}
