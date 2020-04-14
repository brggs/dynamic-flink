package uk.co.brggs.dynamicflink.blocks.uniquethreshold;

import uk.co.brggs.dynamicflink.blocks.Block;
import uk.co.brggs.dynamicflink.blocks.BlockParameterKey;
import uk.co.brggs.dynamicflink.blocks.BlockType;
import uk.co.brggs.dynamicflink.blocks.conditions.EqualCondition;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UniqueThresholdBlockValidatorTest {

    @Test
    void blockWithParameters_IsValid() {
        val block = Block.builder()
                .type(BlockType.UNIQUE_THRESHOLD)
                .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                .windowSize(2)
                .windowSlide(1)
                .parameters(new HashMap<String, String>() {
                    {
                        put(BlockParameterKey.UniqueField, "username");
                        put(BlockParameterKey.Threshold, "3");
                    }
                })
                .aggregationGroupingFields(Collections.singletonList("aggregationField"))
                .build();

        assertTrue(block.validate().isEmpty());
    }

    @Test
    void blockWithoutParameters_IsInvalid() {
        val block = Block.builder()
                .type(BlockType.UNIQUE_THRESHOLD)
                .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                .windowSize(2)
                .windowSlide(1)
                .aggregationGroupingFields(Collections.singletonList("aggregationField"))
                .build();

        assertFalse(block.validate().isEmpty());
    }

    @Test
    void blockWithEmptyParameters_IsInvalid() {
        val block = Block.builder()
                .type(BlockType.UNIQUE_THRESHOLD)
                .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                .windowSize(2)
                .windowSlide(1)
                .parameters(new HashMap<>())
                .aggregationGroupingFields(Collections.singletonList("aggregationField"))
                .build();

        assertFalse(block.validate().isEmpty());
    }

    @Test
    void blockWithoutUniqueField_IsInvalid() {
        val block = Block.builder()
                .type(BlockType.UNIQUE_THRESHOLD)
                .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                .windowSize(2)
                .windowSlide(1)
                .parameters(new HashMap<String, String>() {
                    {
                        put(BlockParameterKey.Threshold, "3");
                    }
                })
                .aggregationGroupingFields(Collections.singletonList("aggregationField"))
                .build();

        assertFalse(block.validate().isEmpty());
    }

    @Test
    void blockWithoutThreshold_IsInvalid() {
        val block = Block.builder()
                .type(BlockType.UNIQUE_THRESHOLD)
                .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                .windowSize(2)
                .windowSlide(1)
                .parameters(new HashMap<String, String>() {
                    {
                        put(BlockParameterKey.UniqueField, "username");
                    }
                })
                .aggregationGroupingFields(Collections.singletonList("aggregationField"))
                .build();

        assertFalse(block.validate().isEmpty());
    }

    @Test
    void blockWithNullUniqueField_IsInvalid() {
        val block = Block.builder()
                .type(BlockType.UNIQUE_THRESHOLD)
                .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                .windowSize(2)
                .windowSlide(1)
                .parameters(new HashMap<String, String>() {
                    {
                        put(BlockParameterKey.UniqueField, null);
                        put(BlockParameterKey.Threshold, "3");
                    }
                })
                .aggregationGroupingFields(Collections.singletonList("aggregationField"))
                .build();

        assertFalse(block.validate().isEmpty());
    }

    @Test
    void blockWithEmptyUniqueField_IsInvalid() {
        val block = Block.builder()
                .type(BlockType.UNIQUE_THRESHOLD)
                .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                .windowSize(2)
                .windowSlide(1)
                .parameters(new HashMap<String, String>() {
                    {
                        put(BlockParameterKey.UniqueField, "");
                        put(BlockParameterKey.Threshold, "3");
                    }
                })
                .aggregationGroupingFields(Collections.singletonList("aggregationField"))
                .build();

        assertFalse(block.validate().isEmpty());
    }

    @Test
    void blockWithZeroThreshold_IsInvalid() {
        val block = Block.builder()
                .type(BlockType.UNIQUE_THRESHOLD)
                .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                .windowSize(2)
                .windowSlide(1)
                .parameters(new HashMap<String, String>() {
                    {
                        put(BlockParameterKey.UniqueField, "username");
                        put(BlockParameterKey.Threshold, "0");
                    }
                })
                .aggregationGroupingFields(Collections.singletonList("aggregationField"))
                .build();

        assertFalse(block.validate().isEmpty());
    }

    @Test
    void blockWithNonNumericThreshold_IsInvalid() {
        val block = Block.builder()
                .type(BlockType.UNIQUE_THRESHOLD)
                .condition(new EqualCondition("destinationIp", "12.23.45.67"))
                .windowSize(2)
                .windowSlide(1)
                .parameters(new HashMap<String, String>() {
                    {
                        put(BlockParameterKey.UniqueField, "username");
                        put(BlockParameterKey.Threshold, "text");
                    }
                })
                .aggregationGroupingFields(Collections.singletonList("aggregationField"))
                .build();

        assertFalse(block.validate().isEmpty());
    }
}
