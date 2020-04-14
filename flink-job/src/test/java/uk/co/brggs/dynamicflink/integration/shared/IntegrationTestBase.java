package uk.co.brggs.dynamicflink.integration.shared;

/**
 * Base class for integration tests, allowing a single test cluster to be reused.
 */
public abstract class IntegrationTestBase {
    protected static final IntegrationTestCluster testCluster = new IntegrationTestCluster();
}
