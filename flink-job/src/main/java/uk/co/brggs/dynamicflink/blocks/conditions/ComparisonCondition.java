package uk.co.brggs.dynamicflink.blocks.conditions;

import uk.co.brggs.dynamicflink.events.InputEvent;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;
import lombok.var;

import java.util.ArrayList;
import java.util.List;

/**
 * Condition to compare the numeric value of a specified field
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ComparisonCondition implements Condition {
    private String key;
    private double thresholdValue;
    private ComparisonType type;

    @Override
    public boolean checkMatch(InputEvent eventContent) {
        var isMatch = false;

        val stringValue = eventContent.getField(key);
        if (stringValue != null && !stringValue.isEmpty()) {
            val actualValue = Float.parseFloat(stringValue);

            switch (type) {
                case GREATER_THAN:
                    isMatch = actualValue > thresholdValue;
                    break;
                case GREATER_THAN_OR_EQUAL:
                    isMatch = actualValue >= thresholdValue;
                    break;
                case LESS_THAN:
                    isMatch = actualValue < thresholdValue;
                    break;
                case LESS_THAN_OR_EQUAL:
                    isMatch = actualValue <= thresholdValue;
                    break;
            }
        }

        return isMatch;
    }

    @Override
    public List<String> validate() {
        val validationErrors = new ArrayList<String>();

        if (key.isEmpty()) {
            validationErrors.add("Empty key specified for ComparisonCondition");
        }

        return validationErrors;
    }
}
