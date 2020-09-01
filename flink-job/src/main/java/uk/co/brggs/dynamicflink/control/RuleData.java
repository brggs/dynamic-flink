package uk.co.brggs.dynamicflink.control;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RuleData {
    private String ruleId;
    private int ruleVersion;
    private String content;
}
