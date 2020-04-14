package uk.co.brggs.dynamicflink.blocks.conditions;

import uk.co.brggs.dynamicflink.events.InputEvent;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;

import java.util.ArrayList;
import java.util.List;

/**
 * Condition to compare the string value of a specified field for an exact value
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EqualCondition implements Condition {
    private String key;
    private String value;

    @Override
    public boolean checkMatch(InputEvent eventContent) {
        return value.equals(eventContent.getField(key));
    }

    @Override
    public List<String> validate() {
        val validationErrors = new ArrayList<String>();

        if (key == null || key.isEmpty()) {
            validationErrors.add("No key specified for EqualCondition.");
        }
        if (value == null || value.isEmpty()) {
            validationErrors.add("No value specified for EqualCondition.");
        }

        return validationErrors;
    }
}
