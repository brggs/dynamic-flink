package uk.co.brggs.dynamicflink.blocks.simpleanomaly;

import lombok.val;
import uk.co.brggs.dynamicflink.blocks.Block;
import uk.co.brggs.dynamicflink.blocks.BlockParameterKey;

import java.util.ArrayList;
import java.util.List;

public class SimpleAnomalyBlockValidator {
    public static List<String> validate(Block block) {
        val validationErrors = new ArrayList<String>();

        if (block.getWindowSize() <= 0) {
            validationErrors.add("WindowSize was not set for Block.");
        }
        if (block.getWindowSlide() > 0) {
            validationErrors.add("WindowSlide is not supported for Simple Anomaly blocks.");
        }

        if (block.getParameters() == null || block.getParameters().isEmpty()) {
            validationErrors.add("Parameters were not set for Simple Anomaly Block.");
        } else {
            val threshold = block.getParameters().get(BlockParameterKey.SimpleAnomalyThreshold);
            if (threshold == null || threshold.isEmpty()) {
                validationErrors.add("SimpleAnomalyThreshold was not set for Simple Anomaly Block.");
            } else {
                try {
                    if (Integer.parseInt(threshold) <= 0) {
                        validationErrors.add(String.format("Invalid anomaly threshold supplied (%s) for Simple Anomaly Block.", threshold));
                    }
                } catch (Exception e) {
                    validationErrors.add(String.format("Invalid anomaly threshold supplied (%s) for Simple Anomaly Block.", threshold));
                }
            }
        }

        return validationErrors;
    }
}
