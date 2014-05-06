package conflator;

public interface Conflator<M extends Message<M>> {

    /**
     * Create a background thread for conflation.
     */
    void daemonize();

    /**
     * Conflate the last {@code messageCount} messages synchronously. If the conflate daemon is active, this conflate
     * is ignored.
     *
     * @param messageCount count of messages to conflate
     */
    void conflate(long messageCount);

    /**
     * Put the message into the conflator.
     *
     * @param message message to send
     */
    void put(M message);

    /**
     * Take message synchronously from the conflator.
     *
     * @return {@link Message} containing the merge of last messages for a given key
     */
    M take();

    /**
     * Stop the background thread.
     */
    void stop();

    /**
     * Is the conflation is done in background or not ?
     *
     * @return {@code true} if it's daemonized
     */
    boolean isDaemonized();

    /**
     * Pause between to iteration of the background thread.
     *
     * @param pauseInMs pause in milliseconds
     */
    void pause(long pauseInMs);

    /**
     * Total of messages inside the conflator. This method could be blocking if needs in order to rightly count the
     * number of messages.
     * @return the total number of messages
     */
    long size();
}
