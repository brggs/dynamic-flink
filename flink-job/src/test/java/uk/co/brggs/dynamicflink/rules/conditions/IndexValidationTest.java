package uk.co.brggs.dynamicflink.rules.conditions;

import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexValidationTest {
    @Test
    void distinctIndices_ShouldBeValid() {
        val indices = Arrays.asList(1, 2, 3);
        assertTrue(IndexValidation.validateIndices(indices).isEmpty());
    }

    @Test
    void nullIndices_ShouldBeInvalid() {
        assertFalse(IndexValidation.validateIndices(null).isEmpty());
    }

    @Test
    void emptyIndices_ShouldBeInvalid() {
        List<Integer> indices = Collections.emptyList();
        assertFalse(IndexValidation.validateIndices(indices).isEmpty());
    }

    @Test
    void duplicateIndices_ShouldBeInvalid() {
        val indices = Arrays.asList(1, 1, 3);
        assertFalse(IndexValidation.validateIndices(indices).isEmpty());
    }

    @Test
    void negativeIndices_ShouldBeInvalid() {
        val indices = Arrays.asList(-1, 1, 3);
        assertFalse(IndexValidation.validateIndices(indices).isEmpty());
    }
}
