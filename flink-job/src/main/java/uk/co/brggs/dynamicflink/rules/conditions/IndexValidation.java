package uk.co.brggs.dynamicflink.rules.conditions;

import lombok.val;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

class IndexValidation {

    static List<String> validateIndices(List<Integer> blockIndices) {
        val validationErrors = new ArrayList<String>();

        if (blockIndices == null || blockIndices.isEmpty()) {
            validationErrors.add("Empty block indices in rule condition");
        } else {
            if (blockIndices.stream().anyMatch(i -> i < 0)) {
                validationErrors.add(
                        "Invalid block indices in rule condition: " + blockIndices.stream()
                                .map(Object::toString)
                                .collect(Collectors.joining(",")));
            }

            val setOfIndices = new HashSet<Integer>(blockIndices);
            if (setOfIndices.size() != blockIndices.size()) {
                validationErrors.add(
                        "Duplicate block indices in rule condition: " + blockIndices.stream()
                                .map(Object::toString)
                                .collect(Collectors.joining(",")));
            }
        }

        return validationErrors;
    }
}
