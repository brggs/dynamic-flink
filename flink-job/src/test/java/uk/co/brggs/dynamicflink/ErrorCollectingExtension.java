package uk.co.brggs.dynamicflink;

import lombok.val;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.InvalidClassException;

/**
 * Extension works with errorCollector from test instance.
 * Error collector is reset or created it is not there before each test execution and verification is performed after each test execution
 */
public class ErrorCollectingExtension implements BeforeTestExecutionCallback, AfterTestExecutionCallback {

    @Override
    public void beforeTestExecution(ExtensionContext context) throws Exception {
        if (!(context.getRequiredTestInstance() instanceof ErrorCollectorAware)) {
            throw new InvalidClassException("Annotated test has to implement ErrorCollectorAware interface");
        }

        val testInstance = (ErrorCollectorAware) context.getRequiredTestInstance();

        if (testInstance.getErrorCollector() == null) {
            testInstance.setErrorCollector(new ErrorCollector());
        }

        testInstance.getErrorCollector().reset();
    }

    @Override
    public void afterTestExecution(ExtensionContext context) throws Exception {
        ErrorCollectorAware testInstance = (ErrorCollectorAware) context.getRequiredTestInstance();
        testInstance.getErrorCollector().verify();
    }

}
