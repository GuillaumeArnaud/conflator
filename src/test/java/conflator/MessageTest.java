package conflator;

import org.junit.Test;

import static org.junit.Assert.*;

public class MessageTest {

    @Test
    public void should_valid_one_letter_message() {
        assertTrue(new SequentialCharacterMessage("key", "a").isValid());
    }

    @Test
    public void should_valid_correct_message() {
        assertTrue(new SequentialCharacterMessage("key", "abcde").isValid());
    }

    @Test
    public void should_not_valid_wrong_message() {
        assertFalse(new SequentialCharacterMessage("key", "azerty").isValid());
    }
}
