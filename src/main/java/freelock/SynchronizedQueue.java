package freelock;

public class SynchronizedQueue<T> {

    private Node<T> head;
    private Node<T> tail;

    public synchronized void put(T t) {
        Node<T> n = new Node<T>(t);
        if (head == null) {
            head = n;
        } else {
            tail.next = n;
        }
        tail = n;
    }

    public synchronized T pick() {
        Node<T> currentHead = head;
        T value = null;
        if (currentHead != null) {
            head = currentHead.next;
            value = currentHead.value;
        }
        return value;
    }

    private static class Node<T> {

        Node<T> next;
        T value;

        public Node(T v) {
            this.value = v;
        }
    }



}