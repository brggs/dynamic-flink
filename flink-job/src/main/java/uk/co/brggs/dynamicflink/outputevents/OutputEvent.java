package uk.co.brggs.dynamicflink.outputevents;

import com.jsoniter.annotation.JsonUnwrapper;
import com.jsoniter.output.JsonStream;
import uk.co.brggs.dynamicflink.blocks.MatchedBlock;
import uk.co.brggs.dynamicflink.events.DateFormatter;
import uk.co.brggs.dynamicflink.events.InputEvent;
import com.jsoniter.annotation.JsonProperty;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Slf4j
public class OutputEvent {
    /**
     * The time at which the rule triggered in the Flink job.
     */
    @Getter(onMethod_ = @JsonProperty(to = {InputEvent.EventTimeField}))
    private String eventTime;

    /**
     * The time of the event which caused the rule to trigger.  For rules containing multiple blocks, this will be the
     * timestamp of the last block to trigger.
     */
    private String matchTime;

    /**
     * The source of this event.
     */
    private String source;

    /**
     * The unique ID of the rule.
     */
    private String matchedRuleId;

    /**
     * The version of the rule.
     */
    private int matchedRuleVersion;

    /**
     * The customer this event relates to.
     */
    private String customer;

    /**
     * The base severity of this event, before customer specific modifiers are applied.
     */
    private int baseSeverity;

    /**
     * Map of Block output, with the Block index as the key.
     */
    private Map<String, BlockOutput> blockOutput;

    /**
     * The key and value used for grouping.
     */
    private String groupBy;

    /**
     * Creates an OutputEvent from a single MatchedBlock.
     */
    public static OutputEvent createFromMatchedBlock(MatchedBlock matchedBlock) {
        return createFromMatchedBlocks(Collections.singletonList(matchedBlock));
    }

    /**
     * Creates an OutputEvent from a number of MatchedBlocks.
     */
    public static OutputEvent createFromMatchedBlocks(List<MatchedBlock> matchedBlocks) {
        val firstBlock = matchedBlocks.get(0);
        val blockOutputMap = new HashMap<String, BlockOutput>();

        var latestMatchTime = 0L;
        for (MatchedBlock matchedBlock : matchedBlocks) {
            blockOutputMap.put(
                    String.valueOf(matchedBlock.getMatchedBlockIndex()),
                    BlockOutput.createFromMatchedBlock(matchedBlock));

            if (matchedBlock.getMatchTime() > latestMatchTime) {
                latestMatchTime = matchedBlock.getMatchTime();
            }
        }

        return OutputEvent.builder()
                .source("Dynamic_Flink")
                .customer(firstBlock.getCustomer())
                .matchedRuleId(firstBlock.getMatchedRuleId())
                .matchedRuleVersion(firstBlock.getMatchedRuleVersion())
                .baseSeverity(firstBlock.getBaseSeverity())
                .eventTime(DateFormatter.getInstance().format(new Date(System.currentTimeMillis())))
                .matchTime(DateFormatter.getInstance().format(new Date(latestMatchTime)))
                .blockOutput(blockOutputMap)
                .groupBy(firstBlock.getGroupBy())
                .build();
    }

    /**
     * Adds the group by data as an extra field on the serialised object.
     *
     * This method is called by Jsoniter during serialisation.
     */
    @SuppressWarnings("unused")
    @JsonUnwrapper
    public void writeCustomGroupByField(JsonStream stream) throws IOException {
        if (groupBy != null && !groupBy.isEmpty()) {

            val groupBySplit = groupBy.split("=");
            if (groupBySplit.length != 2) {
                log.warn("Invalid group by value in output {}", groupBy);
            } else {
                stream.writeObjectField(groupBySplit[0]);
                stream.writeVal(groupBySplit[1]);
            }
        }
    }
}
