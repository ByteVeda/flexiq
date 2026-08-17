package org.byteveda.flexiq.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a {@code @TaskHandler} task's payload for encryption. The generated
 * companion records the {@code "encrypted"} codec on the task, so the producer
 * encrypts the payload on enqueue and the worker decrypts it — both must register
 * a codec under that name: {@code FlexiQ.builder().codec("encrypted", new AesGcmCodec(key))}.
 * The key is supplied at runtime, never in the annotation. Source-retention.
 */
@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface Encrypted {}
