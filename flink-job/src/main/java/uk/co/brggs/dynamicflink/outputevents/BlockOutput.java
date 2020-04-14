package uk.co.brggs.dynamicflink.outputevents;

import uk.co.brggs.dynamicflink.blocks.MatchedBlock;
import uk.co.brggs.dynamicflink.events.DateFormatter;
import lombok.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Represents the output of a block which has triggered.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BlockOutput {

    /**
     * A timestamp indicating when the block match occurred.
     */
    private String matchTime;

    /**
     * A description of why the block matched.
     */
    private String matchMessage;

    /**
     * The events which matched the block, collapsed into EventSummary objects.
     */
    private List<EventSummary> matchingEvents;

    /**
     * Creates a BlockOutput from a MatchedBlock.
     */
    static BlockOutput createFromMatchedBlock(MatchedBlock matchedBlock) {
        return BlockOutput.builder()
                .matchTime(DateFormatter.getInstance().format(new Date(matchedBlock.getMatchTime())))
                .matchMessage(matchedBlock.getMatchMessage())
                .matchingEvents(
                        Optional.ofNullable(matchedBlock.getMatchingEvents())
                                .orElse(new ArrayList<>())
                                .stream()
                                .map(EventSummary::new)
                                .collect(Collectors.toList())
                )
                .build();
    }
}
