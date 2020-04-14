package uk.co.brggs.dynamicflink.blocks.singleevent;

import uk.co.brggs.dynamicflink.blocks.Block;
import uk.co.brggs.dynamicflink.blocks.BlockType;
import uk.co.brggs.dynamicflink.blocks.conditions.EqualCondition;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SingleEventBlockValidatorTest {

    @Test
    void blockWithParameters_IsValid() {
        val block = Block.builder()
                .type(BlockType.SINGLE_EVENT)
                .condition(new EqualCondition("hostname", "importantLaptop"))
                .build();

        assertTrue(block.validate().isEmpty());
    }

    @Test
    void blockWithExtraParameters_IsInvalid() {
        val block = Block.builder()
                .type(BlockType.SINGLE_EVENT)
                .condition(new EqualCondition("hostname", "importantLaptop"))
                .windowSize(2)
                .windowSlide(1)
                .aggregationGroupingFields(Collections.singletonList("aggregationField"))
                .build();

        assertEquals(3, block.validate().size());
    }
}
