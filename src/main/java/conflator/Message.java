package conflator;

public class Message {
    private String key;
    private String body;
    private int mergesCount = 0;

    /**
     * Size of the sequence of successive characters (for instance '0123456789', SEQUENCE_LENGTH=10).
     */
    private final static int SEQUENCE_LENGTH = 10;

    public Message(String key, String body) {
        this.key = key;
        this.body = body;
    }

    public String key() {
        return key;
    }

    public String body() {
        return body;
    }

    public boolean isMerged() {
        return mergesCount > 0;
    }

    public int mergesCount() {
        return mergesCount;
    }

    /**
     * Validate the body of this message.
     *
     * @return {@code true} if the message is valid
     */
    public boolean isValid() {
        return isValid(body);
    }

    /**
     * Validate the given body.
     *
     * @param body the body to check
     * @return {@code true} if the message is valid
     */
    public boolean isValid(String body) {
        if (body == null) return false;
        boolean result = true;
        byte lastChar = (byte) (body.charAt(0) % SEQUENCE_LENGTH);
        for (int i = 1; i < body.length(); i++) {
            result = (lastChar == ((body.charAt(i) - 1) % SEQUENCE_LENGTH));
            if (!result) break;
            lastChar = (byte) (body.charAt(i) % SEQUENCE_LENGTH);
        }
        return result;
    }

    public boolean merge(Message message) {
        boolean mergeable = false;
        String newBody = body + message.body;
        if (isValid(newBody)) {
            body = newBody;
            mergesCount++;
            mergeable = true;
        }
        return mergeable;
    }

}
