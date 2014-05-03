package conflator;

public interface Message<T> {
    String key();

    String body();

    boolean isMerged();

    int mergesCount();

    boolean isValid();

    boolean merge(T message);
}
