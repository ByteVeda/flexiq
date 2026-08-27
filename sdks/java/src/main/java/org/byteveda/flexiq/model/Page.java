package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * One page of a keyset-paginated listing.
 *
 * <p>{@code nextCursor} is {@code null} on the last page — a page shorter than
 * the requested limit means the listing is exhausted — so a walk reads:
 *
 * <pre>{@code
 * JobFilter filter = JobFilter.builder().limit(50).build();
 * String cursor = null;
 * do {
 *     Page<Job> page = queue.listJobsAfter(filter, cursor);
 *     page.items.forEach(this::handle);
 *     cursor = page.nextCursor;
 * } while (cursor != null);
 * }</pre>
 *
 * <p>Pass the cursor back verbatim and treat it as opaque; its format is an
 * implementation detail and may change.
 *
 * @param <T> the listing's element type
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class Page<T> {
    /** This page's rows, newest first. */
    public final List<T> items;

    /** Cursor for the next page, or {@code null} on the last one. */
    public final String nextCursor;

    /**
     * Decoded from the core's JSON page view.
     *
     * @param items this page's rows, newest first
     * @param nextCursor cursor for the next page, or {@code null} on the last one
     */
    @JsonCreator
    public Page(@JsonProperty("items") List<T> items, @JsonProperty("nextCursor") String nextCursor) {
        this.items = items == null ? List.of() : List.copyOf(items);
        this.nextCursor = nextCursor;
    }
}
