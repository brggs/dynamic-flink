package uk.co.brggs.dynamicflink.blocks;

import uk.co.brggs.dynamicflink.blocks.conditions.EqualCondition;
import lombok.val;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BlockTest {

    @Test
    void singleEventBlockWithCondition_IsValid() {
        val block = Block.builder()
                .type(BlockType.SINGLE_EVENT)
                .condition(new EqualCondition("hostname", "importantLaptop"))
                .build();

        assertTrue(block.validate().isEmpty());
    }

    @Test
    void singleEventBlockWithoutCondition_IsInvalid() {
        val block = Block.builder()
                .type(BlockType.SINGLE_EVENT)
                .build();

        assertFalse(block.validate().isEmpty());
    }
}
