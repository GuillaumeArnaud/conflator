package conflator;

public class Message {
    private String key;
    private String body;
    private boolean merged = false;

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

    public boolean merged() {
        return merged;
    }

    public void merge(Message message) {
        body += message.body;
        merged = true;
    }

}
