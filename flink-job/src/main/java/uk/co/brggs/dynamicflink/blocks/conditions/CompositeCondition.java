package uk.co.brggs.dynamicflink.blocks.conditions;

import uk.co.brggs.dynamicflink.events.InputEvent;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Condition allowing other conditions to be combined
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CompositeCondition implements Condition {
    private CompositeType type;
    private List<Condition> conditions;

    @Override
    public boolean checkMatch(InputEvent eventContent) {
        if (type == CompositeType.OR) {
            return conditions.stream().anyMatch(c -> c.checkMatch(eventContent));
        } else {
            return conditions.stream().allMatch(c -> c.checkMatch(eventContent));
        }
    }

    @Override
    public List<String> validate() {
        val validationErrors = new ArrayList<String>();

        if (conditions == null || conditions.isEmpty()) {
            validationErrors.add("No conditions specified for CompositeCondition.");
        } else {
            if (conditions.stream().anyMatch(Objects::isNull)) {
                validationErrors.add("Null condition specified for CompositeCondition.");
            } else {
                conditions.forEach(c -> validationErrors.addAll(c.validate()));
            }
        }

        return validationErrors;
    }
}

