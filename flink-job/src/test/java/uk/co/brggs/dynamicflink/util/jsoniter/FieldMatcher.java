package uk.co.brggs.dynamicflink.util.jsoniter;

import com.jsoniter.any.Any;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.hamcrest.Matcher;

import java.util.function.Predicate;

@AllArgsConstructor(access = AccessLevel.PACKAGE)
public class FieldMatcher<T> implements Predicate<Any> {

    private String field;

    private Matcher<T> matcher;

    @Override
    public boolean test(Any object) {
        if (!object.keys().contains(field)) {
            return false;
        }

        Any value = object.get(field);
        return matcher.matches(value.object());
    }
}