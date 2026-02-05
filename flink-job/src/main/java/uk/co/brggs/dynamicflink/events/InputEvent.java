package uk.co.brggs.dynamicflink.events;

import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import com.jsoniter.output.JsonStream;
import lombok.Getter;
import lombok.NonNull;

import java.io.IOException;

/**
 * Holds an input event, and caches requests for field values.
 */
public class InputEvent {
    public static final String EventTimeField = "@timestamp";
    public static final String CustomerField = "customer";

    @Getter
    private final String content;

    private final Any parsedContent;

    public InputEvent(@NonNull String content) throws IOException {
        this.content = content;
        parsedContent = JsonIterator.parse(content).readAny();
    }

    public InputEvent(@NonNull Any content) {
        this.content = JsonStream.serialize(content);
        parsedContent = content;
    }

    /**
     * Fetches the specified field from the event.
     *
     * @param fieldName The field we want to find a value for
     * @return The value found, or null if it does not exist
     */
    public String getField(String fieldName) {
        var value = parsedContent.get(fieldName).toString();
        if (value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }
}
