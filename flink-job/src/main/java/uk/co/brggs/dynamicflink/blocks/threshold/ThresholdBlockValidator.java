package uk.co.brggs.dynamicflink.blocks.threshold;

import lombok.val;
import uk.co.brggs.dynamicflink.blocks.Block;
import uk.co.brggs.dynamicflink.blocks.BlockParameterKey;
import uk.co.brggs.dynamicflink.blocks.WindowedBlockValidator;

import java.util.List;

public class ThresholdBlockValidator {
    public static List<String> validate(Block block) {
        val validationErrors = WindowedBlockValidator.validateWithAggregationFields(block);

        if (block.getParameters() == null) {
            validationErrors.add("No parameters were set for Threshold Block.");
        } else {
            val threshold = block.getParameters().get(BlockParameterKey.Threshold);
            if (threshold == null || threshold.isEmpty()) {
                validationErrors.add("Threshold was not set for Threshold Block.");
            } else {
                try {
                    if (Integer.parseInt(threshold) <= 0) {
                        validationErrors.add(String.format("Invalid threshold supplied (%s) for Threshold Block.", threshold));
                    }
                } catch (Exception e) {
                    validationErrors.add(String.format("Invalid threshold supplied (%s) for Threshold Block.", threshold));
                }
            }
        }

        return validationErrors;
    }
}
