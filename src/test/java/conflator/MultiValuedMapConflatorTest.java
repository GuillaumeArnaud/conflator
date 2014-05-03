package conflator;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MultiValuedMapConflatorTest {
    MultiValuedMapConflator conflator;

    @Test(timeout = 1000)
    public void two_unmergeable_messages_should_be_remain_unmergeable() {
        Message message1 = new SequentialCharacterMessage("key", "a");
        Message message2 = new SequentialCharacterMessage("key", "b");
        Message message3 = new SequentialCharacterMessage("key", "d");
        Message message4 = new SequentialCharacterMessage("key", "e");


        List<SequentialCharacterMessage> messages = conflator.merge(Lists.newArrayList(message1, message2, message3, message4));

        assertNotNull(messages);
        assertEquals(messages.size(), 3);
        assertEquals(messages.get(0).body(), "ab");
        assertEquals(messages.get(1).body(), "d");
        assertEquals(messages.get(2).body(), "e");
    }

    @Test(timeout = 1000)
    public void two_mergeable_messages_should_be_merged() {
        Message message1 = new SequentialCharacterMessage("key", "a");
        Message message2 = new SequentialCharacterMessage("key", "b");

        List<Message> messages = conflator.merge(Lists.newArrayList(message1, message2));

        assertNotNull(messages);
        assertEquals(messages.size(), 1);
        assertEquals(messages.get(0).body(), "ab");
    }

    @Before
    public void setUp() {
        conflator = new MultiValuedMapConflator<SequentialCharacterMessage>(true);
    }

    @After
    public void tearDown() {
        conflator.stop();
    }
}
