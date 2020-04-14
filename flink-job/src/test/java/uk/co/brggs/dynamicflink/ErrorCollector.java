package uk.co.brggs.dynamicflink;

import org.junit.runners.model.MultipleFailureException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class ErrorCollector {

    private List<Throwable> errors = new ArrayList<>();

    public void verify() throws Exception {
        MultipleFailureException.assertEmpty(errors);
    }

    public void reset() {
        errors = new ArrayList<>();
    }

    public <V> V addErrorIfFails(Callable<V> callable) {
        try {
            return callable.call();
        }
        catch (Throwable e) {
            addError(e);
        }
        return null;
    }

    /**
     * Adds a Throwable to the table.  Execution continues, but the test will fail at the end.
     */
    public void addError(Throwable error) {
        errors.add(error);
    }
}