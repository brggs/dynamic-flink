package uk.co.brggs.dynamicflink.blocks;

import lombok.val;

import java.util.ArrayList;
import java.util.List;

public class WindowedBlockValidator {
    public static List<String> validate(Block block) {
        val validationErrors = new ArrayList<String>();

        if (block.getWindowSize() <= 0) {
            validationErrors.add("WindowSize was not set for Block.");
        }
        if (block.getWindowSlide() <= 0) {
            validationErrors.add("WindowSlide was not set for Block.");
        }

        return validationErrors;
    }

    public static List<String> validateWithAggregationFields(Block block) {
        val validationErrors = validate(block);

        if (block.getAggregationGroupingFields() == null || block.getAggregationGroupingFields().isEmpty()) {
            validationErrors.add("No aggregationGroupingFields supplied for Block.");
        } else {
            if (block.getAggregationGroupingFields().stream().anyMatch(f -> f == null || f.isEmpty())) {
                validationErrors.add("Null or empty aggregationGroupingField supplied for Block.");
            }
        }

        return validationErrors;
    }

}
