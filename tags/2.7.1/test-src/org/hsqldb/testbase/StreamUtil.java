package org.hsqldb.testbase;

import java.util.Enumeration;
import java.util.Spliterator;
import java.util.Spliterators.AbstractSpliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class StreamUtil {

    public static class EnumerationSpliterator<T> extends AbstractSpliterator<T> {

        private final Enumeration<T> enumeration;

        public EnumerationSpliterator(long est, int additionalCharacteristics, Enumeration<T> enumeration) {
            super(est, additionalCharacteristics);
            this.enumeration = enumeration;
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            if (enumeration.hasMoreElements()) {
                action.accept(enumeration.nextElement());
                return true;
            }
            return false;
        }

        @Override
        public void forEachRemaining(Consumer<? super T> action) {
            while (enumeration.hasMoreElements()) {
                action.accept(enumeration.nextElement());
            }
        }
    }

    public static <T> Stream<T> streamOf(Enumeration<T> enumeration) {
        EnumerationSpliterator<T> spliterator
                = new EnumerationSpliterator<>(Long.MAX_VALUE, Spliterator.ORDERED, enumeration);
        Stream<T> stream = StreamSupport.stream(spliterator, false);

        return stream;
    }
}
