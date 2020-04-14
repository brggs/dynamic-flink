package uk.co.brggs.dynamicflink.events;

import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertEquals;

/**
 * InputEvent Tester.
 */
public class InputEventTest {

    /**
     * Method: getContent()
     */
    @Test
    public void testGetContent() throws Exception {

        String jsonStr = "{\"field\": 123}";
        Any json = JsonIterator.deserialize(jsonStr);

        InputEvent event;
        event = new InputEvent(jsonStr);
        assertEquals(jsonStr, event.getContent());

        event = new InputEvent(json);
        assertEquals(jsonStr, event.getContent());

    }
}
