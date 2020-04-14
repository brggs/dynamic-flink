package uk.co.brggs.dynamicflink.blocks;

/**
 * The types of blocks supported by the job.
 */
public enum BlockType {

    /**
     * A block which matches a single event.
     */
    SINGLE_EVENT,

    /**
     * A block which counts matching events.
     */
    THRESHOLD,

    /**
     * A block which counts the number of unique values in a given field, in matching events.
     */
    UNIQUE_THRESHOLD,

    /**
     * A block which looks for changes in the count of matching events.
     */
    SIMPLE_ANOMALY,

    /**
     * A block which looks for drops to zero in the count of matching events.
     */
    DROP_TO_ZERO
}
