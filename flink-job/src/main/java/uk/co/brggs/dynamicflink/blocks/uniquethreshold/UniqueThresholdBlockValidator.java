package uk.co.brggs.dynamicflink.blocks.uniquethreshold;

import uk.co.brggs.dynamicflink.blocks.Block;
import uk.co.brggs.dynamicflink.blocks.BlockParameterKey;
import uk.co.brggs.dynamicflink.blocks.WindowedBlockValidator;
import lombok.val;

import java.util.List;

public class UniqueThresholdBlockValidator {
    public static List<String> validate(Block block) {
        val validationErrors = WindowedBlockValidator.validateWithAggregationFields(block);

        if (block.getParameters() == null || block.getParameters().isEmpty()) {
            validationErrors.add("Parameters were not set for Unique Threshold Block.");
        } else {
            val uniqueField = block.getParameters().get(BlockParameterKey.UniqueField);
            if (uniqueField == null || uniqueField.isEmpty()) {
                validationErrors.add("UniqueField was not set for Unique Threshold Block.");
            }

            val threshold = block.getParameters().get(BlockParameterKey.Threshold);
            if (threshold == null || threshold.isEmpty()) {
                validationErrors.add("Threshold was not set for Unique Threshold Block.");
            } else {
                try {
                    if (Integer.parseInt(threshold) <= 0) {
                        validationErrors.add(String.format("Invalid threshold supplied (%s) for Unique Threshold Block.", threshold));
                    }
                } catch (Exception e) {
                    validationErrors.add(String.format("Invalid threshold supplied (%s) for Unique Threshold Block.", threshold));
                }
            }
        }

        return validationErrors;
    }
}
