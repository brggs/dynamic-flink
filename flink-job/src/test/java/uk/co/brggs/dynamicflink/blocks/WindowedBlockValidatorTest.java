package uk.co.brggs.dynamicflink.blocks;

import uk.co.brggs.dynamicflink.blocks.conditions.EqualCondition;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WindowedBlockValidatorTest {

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
    void blockWithoutWindowParams_IsInvalid() {
        val block = Block.builder()
                .type(BlockType.THRESHOLD)
                .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                .parameters(Collections.singletonMap(BlockParameterKey.Threshold, "2"))
                .aggregationGroupingFields(Collections.singletonList("aggregationField"))
                .build();

        assertEquals(2, block.validate().size());
    }

    @Test
    void blockWithInvalidWindowParams_IsInvalid() {
        val block = Block.builder()
                .type(BlockType.THRESHOLD)
                .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                .windowSize(0)
                .windowSlide(0)
                .parameters(Collections.singletonMap(BlockParameterKey.Threshold, "2"))
                .aggregationGroupingFields(Collections.singletonList("aggregationField"))
                .build();

        assertEquals(2, block.validate().size());
    }

    @Test
    void blockWithoutAggregationGroupingFields_IsInvalid() {
        val block = Block.builder()
                .type(BlockType.THRESHOLD)
                .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                .windowSize(2)
                .windowSlide(1)
                .parameters(Collections.singletonMap(BlockParameterKey.Threshold, "2"))
                .build();

        assertFalse(block.validate().isEmpty());
    }

    @Test
    void blockWithEmptyAggregationGroupingFields_IsInvalid() {
        val block = Block.builder()
                .type(BlockType.THRESHOLD)
                .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                .windowSize(2)
                .windowSlide(1)
                .parameters(Collections.singletonMap(BlockParameterKey.Threshold, "2"))
                .aggregationGroupingFields(Collections.emptyList())
                .build();

        assertFalse(block.validate().isEmpty());
    }

    @Test
    void blockWithAggregationGroupingFieldsWithNullValue_IsInvalid() {
        val block = Block.builder()
                .type(BlockType.THRESHOLD)
                .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                .windowSize(2)
                .windowSlide(1)
                .parameters(Collections.singletonMap(BlockParameterKey.Threshold, "2"))
                .aggregationGroupingFields(Collections.singletonList(null))
                .build();

        assertFalse(block.validate().isEmpty());
    }
}
