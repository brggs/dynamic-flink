package uk.co.brggs.dynamicflink.control;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Defines the format for control responses sent back to the API, indicating what action was taken and the current
 * status of the job state.
 */
@Data
@NoArgsConstructor
public class ControlOutput {
    private String version = ControlOutput.class.getPackage().getImplementationVersion();
    private String timestamp = Instant.now().toString();
    private String inputTimestamp;
    private ControlInputType action;
    private ControlOutputStatus status;
    private String ruleId;
    private int ruleVersion;
    private int controlSchemaVersion;
    private String content;

    public ControlOutput(ControlInput controlInput, ControlOutputStatus controlOutputStatus) {
        this.inputTimestamp = controlInput.getTimestamp();
        this.action = controlInput.getType();
        this.ruleId = controlInput.getRuleId();
        this.ruleVersion = controlInput.getRuleVersion();
        this.status = controlOutputStatus;
    }

    public ControlOutput(ControlInput controlInput, ControlOutputStatus controlOutputStatus, String content) {
        this(controlInput, controlOutputStatus);
        this.content = content;
    }
}
