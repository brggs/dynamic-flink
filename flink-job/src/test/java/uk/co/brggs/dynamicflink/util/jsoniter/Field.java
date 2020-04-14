package uk.co.brggs.dynamicflink.util.jsoniter;

import org.hamcrest.core.IsEqual;


public class Field {

    private String field;

    private Field(String field) {
        this.field = field;
    }

    public static Field field(String field) {
        return new Field(field);
    }

    public <T> FieldMatcher isEqual(T value) {
        return new FieldMatcher(field, new IsEqual<>(value));
    }
}
