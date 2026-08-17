package org.byteveda.flexiq.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a {@code @TaskHandler} task's payload for compression. The generated
 * companion records the {@code "compressed"} codec on the task; both producer and
 * worker must register a codec under that name:
 * {@code FlexiQ.builder().codec("compressed", new GzipCodec())}. When combined
 * with {@link Encrypted}, compression runs first (compress, then encrypt).
 * Source-retention.
 */
@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface Compressed {}
