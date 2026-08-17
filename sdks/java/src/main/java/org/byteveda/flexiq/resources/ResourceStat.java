package org.byteveda.flexiq.resources;

/**
 * Per-resource counters.
 *
 * @param created how many instances have been built
 * @param disposed how many have been disposed
 * @param active currently-live instances ({@code created - disposed})
 */
public record ResourceStat(long created, long disposed, long active) {}
