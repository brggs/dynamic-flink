package uk.co.brggs.dynamicflink.blocks;

import uk.co.brggs.dynamicflink.blocks.conditions.Condition;
import uk.co.brggs.dynamicflink.blocks.droptozero.DropToZeroBlockValidator;
import uk.co.brggs.dynamicflink.blocks.simpleanomaly.SimpleAnomalyBlockValidator;
import uk.co.brggs.dynamicflink.blocks.singleevent.SingleEventBlockValidator;
import uk.co.brggs.dynamicflink.blocks.threshold.ThresholdBlockValidator;
import uk.co.brggs.dynamicflink.blocks.uniquethreshold.UniqueThresholdBlockValidator;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Represents a block which will match certain events, used in rules.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Block {

    /**
     * The type of block, which determines how events are matched.
     */
    private BlockType type;

    /**
     * The conditions which an event must match in order to be processed by the block.
     */
    private Condition condition;

    /**
     * The size of the window used to process events within the block, in milliseconds.
     * Does not apply to single event blocks.
     */
    private int windowSize;

    /**
     * The slide of the window used to process events within the block, in milliseconds.
     * Does not apply to single event blocks.
     */
    private int windowSlide;

    /**
     * A list of fields which will be used to create a key which group events together in blocks which aggregate events.
     * One event is kept per key, along with a count.
     * Does not apply to single event blocks.
     */
    private List<String> aggregationGroupingFields;

    /**
     * Block specific parameters.
     */
    private Map<String, String> parameters;

    public List<String> validate() {
        val validationErrors = new ArrayList<String>();

        if (condition == null) {
            validationErrors.add("No condition specified for Block.");
        } else {
            validationErrors.addAll(condition.validate());
        }

        switch (type) {
            case SINGLE_EVENT:
                validationErrors.addAll(SingleEventBlockValidator.validate(this));
                break;
            case THRESHOLD:
                validationErrors.addAll(ThresholdBlockValidator.validate(this));
                break;
            case UNIQUE_THRESHOLD:
                validationErrors.addAll(UniqueThresholdBlockValidator.validate(this));
                break;
            case SIMPLE_ANOMALY:
                validationErrors.addAll(SimpleAnomalyBlockValidator.validate(this));
                break;
            case DROP_TO_ZERO:
                validationErrors.addAll(DropToZeroBlockValidator.validate(this));
                break;
            default:
                validationErrors.add(String.format("Unknown block type %s.", type));
                break;
        }

        return validationErrors;
    }
}
