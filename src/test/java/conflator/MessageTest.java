package conflator;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class MessageTest {

    public void should_valid_one_letter_message() {
        assertTrue(new Message("key", "a").isValid());
    }

    @Test
    public void should_valid_correct_message() {
        assertTrue(new Message("key", "abcde").isValid());
    }

    @Test
    public void should_not_valid_wrong_message() {
        assertFalse(new Message("key", "azerty").isValid());
    }
}
