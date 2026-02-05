package uk.co.brggs.dynamicflink.functions;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

/**
 * Wraps a Flat Map function in a try catch to ensure all exceptions are handled.
 *
 * @param <T> The input type
 * @param <R> The output type
 */
@Data
@RequiredArgsConstructor
@Slf4j
public class SafeFlatMapFunction<T, R> implements FlatMapFunction<T, R>, ResultTypeQueryable {
    private final FlatMapFunction<T, R> flatMapper;
    private final Class<R> clazz;

    @Override
    public void flatMap(T value, Collector<R> out) {
        try {
            flatMapper.flatMap(value, out);
        } catch (Exception e) {
            log.error("Exception occurred in FlatMapFunction.", e);
            e.printStackTrace();
        }
    }

    @Override
    public TypeInformation getProducedType() {
        return TypeInformation.of(clazz);
    }
}
