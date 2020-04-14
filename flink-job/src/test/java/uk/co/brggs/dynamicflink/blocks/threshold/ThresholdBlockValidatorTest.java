package uk.co.brggs.dynamicflink.blocks.threshold;

import uk.co.brggs.dynamicflink.blocks.Block;
import uk.co.brggs.dynamicflink.blocks.BlockParameterKey;
import uk.co.brggs.dynamicflink.blocks.BlockType;
import uk.co.brggs.dynamicflink.blocks.conditions.EqualCondition;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThresholdBlockValidatorTest {

    @Test
    void blockWithParameters_IsValid() {
        val block = Block.builder()
                .type(BlockType.THRESHOLD)
                .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                .windowSize(2)
                .windowSlide(1)
                .parameters(Collections.singletonMap(BlockParameterKey.Threshold, "2"))
                .aggregationGroupingFields(Collections.singletonList("aggregationField"))
                .build();

        assertTrue(block.validate().isEmpty());
    }

    @Test
    void blockWithoutThreshold_IsInvalid() {
        val block = Block.builder()
                .type(BlockType.THRESHOLD)
                .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                .windowSize(2)
                .windowSlide(1)
                .aggregationGroupingFields(Collections.singletonList("aggregationField"))
                .build();

        assertFalse(block.validate().isEmpty());
    }

    @Test
    void blockWithZeroThreshold_IsInvalid() {
        val block = Block.builder()
                .type(BlockType.THRESHOLD)
                .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                .windowSize(2)
                .windowSlide(1)
                .parameters(Collections.singletonMap(BlockParameterKey.Threshold, "0"))
                .aggregationGroupingFields(Collections.singletonList("aggregationField"))
                .build();

        assertFalse(block.validate().isEmpty());
    }

    @Test
    void blockWithNonNumericThreshold_IsInvalid() {
        val block = Block.builder()
                .type(BlockType.THRESHOLD)
                .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                .windowSize(2)
                .windowSlide(1)
                .parameters(Collections.singletonMap(BlockParameterKey.Threshold, "text"))
                .aggregationGroupingFields(Collections.singletonList("aggregationField"))
                .build();

        assertFalse(block.validate().isEmpty());
    }
}
