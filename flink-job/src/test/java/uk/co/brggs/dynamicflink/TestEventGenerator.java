package uk.co.brggs.dynamicflink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import lombok.Builder;
import lombok.val;

import java.time.Instant;

@Builder
public class TestEventGenerator {

    @Builder.Default
    private final String customer = "CustomerOne";

    @Builder.Default
    private final Instant startTime = Instant.now();

    private final ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    public String generate(String... fields) throws JsonProcessingException {
        return generate(0, fields);
    }

    public String generate(int timeOffset, String... fields) throws JsonProcessingException {
        val node = JsonNodeFactory.instance.objectNode();

        node.put("@timestamp", startTime.plusSeconds(timeOffset).toString());
        node.put("customer", customer);

        if (fields.length % 2 != 0) {
            throw new IllegalArgumentException("An even number of string args must be supplied.");
        }

        for (int i = 0; i < fields.length; i = i + 2) {
            node.set(fields[i], JsonNodeFactory.instance.textNode(fields[i + 1]));
        }

        return mapper.writeValueAsString(node);
    }
}
