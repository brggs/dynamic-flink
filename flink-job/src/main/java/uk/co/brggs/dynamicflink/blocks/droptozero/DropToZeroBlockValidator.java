package uk.co.brggs.dynamicflink.blocks.droptozero;

import lombok.val;
import uk.co.brggs.dynamicflink.blocks.Block;

import java.util.ArrayList;
import java.util.List;

public class DropToZeroBlockValidator {
    public static List<String> validate(Block block) {
        val validationErrors = new ArrayList<String>();

        if (block.getWindowSize() <= 0) {
            validationErrors.add("WindowSize was not set for Block.");
        }
        if (block.getWindowSlide() > 0) {
            validationErrors.add("WindowSlide is not supported for DropToZero blocks.");
        }

        return validationErrors;
    }
}
