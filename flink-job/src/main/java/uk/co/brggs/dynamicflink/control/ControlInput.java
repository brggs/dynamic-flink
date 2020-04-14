package uk.co.brggs.dynamicflink.control;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import uk.co.brggs.dynamicflink.rules.Rule;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

/**
 * Holds data relating to command/control events for the system
 */
@Data
@NoArgsConstructor
public class ControlInput implements Serializable {
    private String timestamp;
    private ControlInputType type;
    private String ruleId;
    private int ruleVersion;
    private int controlSchemaVersion;
    private String content;

    public ControlInput(ControlInputType type) {
        this.timestamp = Instant.now().toString();
        this.type = type;
        this.controlSchemaVersion = 1;
    }

    public ControlInput(ControlInputType type, String ruleId, int ruleVersion, String content) {
        this.timestamp = Instant.now().toString();
        this.type = type;
        this.ruleId = ruleId;
        this.ruleVersion = ruleVersion;
        this.controlSchemaVersion = 1;
        this.content = content;
    }

    /**
     * Validates the content of the input.
     *
     * @return A list of validation errors.  An empty list if the object is valid.
     */
    public List<String> validate() {
        val validationErrors = new ArrayList<String>();

        if (controlSchemaVersion != 1) {
            validationErrors.add(format("Schema version %d is not supported", controlSchemaVersion));
        }

        try {
            try {
                if (timestamp == null) {
                    validationErrors.add("Timestamp is null.");
                }
                else {
                    Instant.parse(timestamp);
                }
            } catch (DateTimeParseException e) {
                validationErrors.add(format("Timestamp is invalid: %s", timestamp));
            }

            switch (type) {
                case ADD_RULE:
                    if (ruleIdIsInvalid(ruleId)) {
                        validationErrors.add(format("Rule ID is invalid: %s", ruleId));
                    }
                    if (ruleVersionIsInvalid(ruleVersion)) {
                        validationErrors.add(format("Rule version is invalid: %d", ruleVersion));
                    }
                    if (content == null || content.isEmpty()) {
                        validationErrors.add("Rule content is empty.");
                    }

                    try {
                        val rule = new ObjectMapper().readValue(content, Rule.class);
                        validationErrors.addAll(rule.validate());

                        // Check the rule ID and version are not set in the content (or are the same as in the control event)
                        if (rule.getId() != null && !rule.getId().equals(ruleId)) {
                            validationErrors.add(format(
                                    "Rule ID specified in content (%s) did not match the ID in the control event (%s).",
                                    rule.getId(),
                                    ruleId));
                        }
                        if (rule.getVersion() != 0 && rule.getVersion() != ruleVersion) {
                            validationErrors.add(format(
                                    "Rule version specified in content (%d) did not match the version in the control event (%d).",
                                    rule.getVersion(),
                                    ruleVersion));
                        }
                    } catch (IOException e) {
                        validationErrors.add(format("Failed to deserialise rule: %s", e));
                    }

                    break;
                case REMOVE_RULE:
                    if (ruleIdIsInvalid(ruleId)) {
                        validationErrors.add(format("Rule ID is invalid: %s", ruleId));
                    }
                    if (ruleVersionIsInvalid(ruleVersion)) {
                        validationErrors.add(format("Rule version is invalid: %d", ruleVersion));
                    }
                    break;
                case QUERY_STATUS:
                    // No further validation required
                    break;
                default:
                    validationErrors.add("Invalid ControlType" + type);
                    break;
            }
        } catch (Exception e) {
            validationErrors.add(format("Error validating ControlInput %s", e));
        }

        return validationErrors;
    }

    private boolean ruleIdIsInvalid(String ruleId) {
        return ruleId == null || ruleId.isEmpty();
    }

    private boolean ruleVersionIsInvalid(int ruleVersion) {
        return ruleVersion <= 0;
    }
}
