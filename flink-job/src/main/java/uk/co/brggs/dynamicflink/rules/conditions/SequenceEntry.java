package uk.co.brggs.dynamicflink.rules.conditions;

import lombok.*;

@Data
@NoArgsConstructor
@RequiredArgsConstructor
@AllArgsConstructor
public class SequenceEntry {

    /**
     * The index of the block in the rule's condition list.
     */
    @NonNull
    private int blockIndex;

    /**
     * Whether the block should appear, or should not appear in the sequence.  Note, the latter is only valid at the end of a
     * sequence.
     */
    @Getter
    private boolean notPresent = false;
}
