package uk.co.brggs.dynamicflink;

/**
 * Interface for tests which contain errorCollector.
 * It might be used to prevent failing the test (method) on first assertion.
 * Instead test can continue till the end, in that case it should be used with @link {@link ErrorCollectingExtension}
 */
public interface ErrorCollectorAware {

    ErrorCollector getErrorCollector();

    void setErrorCollector(ErrorCollector collector);
}
