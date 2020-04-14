package uk.co.brggs.dynamicflink.blocks.conditions;

import uk.co.brggs.dynamicflink.events.InputEvent;
import lombok.*;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Condition to compare the string value of a specified field for a match against a regex
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RegexCondition implements Condition {
    private String key;
    private String regex;

    @Override
    public boolean checkMatch(InputEvent eventContent) {
        val value = eventContent.getField(key);

        if (value != null) {
            val p = Pattern.compile(regex);
            val m = p.matcher(value);

            return m.find();
        } else {
            return false;
        }
    }

    @Override
    public List<String> validate() {
        val validationErrors = new ArrayList<String>();

        if (key == null || key.isEmpty()) {
            validationErrors.add("No key specified for RegexCondition.");
        }
        if (regex == null || regex.isEmpty()) {
            validationErrors.add("No regex specified for RegexCondition.");
        }

        return validationErrors;
    }
}
