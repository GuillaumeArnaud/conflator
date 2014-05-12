package freelock;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Created by william on 05/05/2014.
 */
public class LockFreeQueue<T> {

    private static final Unsafe UNSAFE;
    private static final long headOffset;
    private static final long tailOffset;


    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
            Class k = LockFreeQueue.class;
            headOffset = UNSAFE.objectFieldOffset
                    (k.getDeclaredField("head"));
            tailOffset = UNSAFE.objectFieldOffset
                    (k.getDeclaredField("tail"));

        } catch (Exception e) {
            throw new Error(e);
        }
    }

    private Node<T> head;
    private Node<T> tail;

    public void put(T v) {
        Node<T> n = new Node<>(v);
        for (; ; ) {
            if (head == null) UNSAFE.compareAndSwapObject(this, headOffset, null, n);
            if (UNSAFE.compareAndSwapObject(this, tailOffset, tail, n)) {
                if (tail != null) tail.next = n;
                break;
            }
        }
    }

    public T pick() {
        for (; ; ) {
            Node<T> h = head;
            if (h == null) return null;
            if (UNSAFE.compareAndSwapObject(this, headOffset, h, h.next)) {
                return h.value;
            }
        }
    }

    private static class Node<T> {

        Node<T> next;
        T value;

        public Node(T v) {
            this.value = v;
        }
    }


}
