package uk.co.brggs.dynamicflink.util.jsoniter;

import com.jsoniter.any.Any;
import org.assertj.core.api.IterableAssert;
import org.assertj.core.api.ObjectAssert;

import javax.annotation.CheckReturnValue;

public class AnyAssert extends IterableAssert<Any> {

    public AnyAssert(Any actual) {
        super(actual);
    }

    @CheckReturnValue
    public static AnyAssert assertThat(Any actual) {
        return new AnyAssert(actual);
    }

    public AnyAssert field(String field) {
        Any that = (Any) actual;
        return new AnyAssert(that.get(field));
    }

    @Override
    public AnyAssert isEqualTo(Object expected) {
        /**
         * This is to avoid compering:
         * - {@link com.jsoniter.any.StringAny}
         * - {@link com.jsoniter.any.StringLazyAny}
         * which fails
         */
        ObjectAssert<Object> objectAssert = new ObjectAssert<>(((Any) actual).object());
        if (expected instanceof Any) {
            expected = ((Any) expected).object();
        }
        objectAssert.isEqualTo(expected);
        return this;
    }
}
