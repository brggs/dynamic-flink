package uk.co.brggs.dynamicflink.blocks.droptozero;

import uk.co.brggs.dynamicflink.blocks.Block;
import uk.co.brggs.dynamicflink.blocks.BlockType;
import uk.co.brggs.dynamicflink.blocks.conditions.EqualCondition;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertTrue;

class DropToZeroBlockValidatorTest {

    @Test
    void blockWithParameters_IsValid() {
        val block = Block.builder()
                .type(BlockType.DROP_TO_ZERO)
                .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                .windowSize(2)
                .aggregationGroupingFields(Collections.singletonList("aggregationField"))
                .build();

        assertTrue(block.validate().isEmpty());
    }
}
