package conflator;

import com.google.common.collect.ArrayListMultimap;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.Thread.sleep;


public class Conflator {

    /**
     * The multivalued map which stores all messages indexed by key.
     */
    ArrayListMultimap<String, Message> data = ArrayListMultimap.create();

    /**
     * The queue which store the keys of the new messages. The queue, at an instant, must contain only one version of
     * the key.
     */
    LinkedBlockingQueue<String> cursors = new LinkedBlockingQueue<>();

    /**
     * The blocking queue which receives message from external and before the dispatch to {@link #data}.
     */
    BlockingQueue<Message> queue = new LinkedBlockingDeque<>();

    /**
     * Lock for accessing to {@link #data}.
     */
    Lock lock = new ReentrantLock();

    long pauseInMs = 0;
    private volatile boolean stopped = false;

    public Conflator() {
        // Start a thread for taking message on queue and put it in data concurrently to user action
        new Thread(() -> {
            while (!stopped) {
                try {
                    Message d = queue.take();
                    // lock for updating data
                    lock.lock();
                    List<Message> values = data.get(d.key());
                    int size = values.size();
                    if (size == 0) {
                        // there is no message currently for this key, so we can update the cursor
                        cursors.put(d.key());
                    }
                    // add the message to data
                    values.add(d);
                    lock.unlock();
                    // end of data updating
                    sleep(pauseInMs);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    /**
     * Put the message into the conflator.
     * The message is put into a {@link #queue} and then, a background thread will put it into {@link #data} after a
     * merge step (if needed).
     *
     * @param message message to send
     * @throws InterruptedException
     */
    public void put(Message message) throws InterruptedException {
        queue.put(message);
    }

    /**
     * Take message synchronously from the conflator.
     * The message is took from {@link #data}. If more than one message is found, all messages are merged.
     *
     * @return {@link Message} containing the merged of last messages for a given key
     * @throws InterruptedException
     */
    public Message take() throws InterruptedException {
        // take the new key of the next message
        String take = cursors.take();
        int size = 0; // number of message retrieving from {@link #data}
        List<Message> message = null;
        while (size == 0) {
            lock.lock();
            message = data.removeAll(take); // remove messages from {@link #data}
            lock.unlock();
            size = message.size(); // WARN the size could be 0 if the {@link #cursor} is updated before the update of {@link #data}. Should be very rare.
        }
        return merge(message);
    }

    /**
     * Merge the first element with all other from the list.
     *
     * @param elements list of {@link Message} to merge
     * @return the merged {@link Message}
     */
    private Message merge(List<Message> elements) {
        Message first = null;
        for (Message element : elements) {
            if (first == null) first = element;
            else first.merge(element);
        }
        return first;
    }

    /**
     * Stop the background thread.
     */
    public void stop() {
        this.stopped = true;
    }

    /**
     * Pause between to iteration of the background thread ({@link #Conflator()}.
     *
     * @param pauseInMs pause in milliseconds
     */
    public void pause(long pauseInMs) {
        this.pauseInMs = pauseInMs;
    }
}
