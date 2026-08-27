package org.byteveda.flexiq.predicates;

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;

/**
 * Ready-made {@link EnqueueGate}s for common scheduling and filtering policies.
 * Time-based recipes {@code defer} out-of-window enqueues to the next open
 * moment; filter recipes {@code skip} non-matching payloads.
 */
public final class Recipes {
    private Recipes() {}

    private static final LocalTime DEFAULT_OPEN = LocalTime.of(9, 0);
    private static final LocalTime DEFAULT_CLOSE = LocalTime.of(17, 0);
    private static final Set<DayOfWeek> WEEKDAYS =
            EnumSet.of(DayOfWeek.MONDAY, DayOfWeek.TUESDAY, DayOfWeek.WEDNESDAY, DayOfWeek.THURSDAY, DayOfWeek.FRIDAY);

    /**
     * Allow weekday 09:00–17:00 in {@code zone}; otherwise defer to the next opening.
     *
     * @param zone the zone the window is read in, so the policy follows local time
     * @return the gate
     */
    public static EnqueueGate businessHours(ZoneId zone) {
        return businessHours(zone, DEFAULT_OPEN, DEFAULT_CLOSE);
    }

    /**
     * Allow Mon–Fri within {@code [open, close)} in {@code zone}; otherwise defer to the next opening.
     *
     * @param zone the zone the window is read in, so the policy follows local time
     * @param open when the window opens, inclusive
     * @param close when it closes, exclusive
     * @return the gate
     */
    public static EnqueueGate businessHours(ZoneId zone, LocalTime open, LocalTime close) {
        return context -> {
            ZonedDateTime now = ZonedDateTime.now(zone);
            LocalTime time = now.toLocalTime();
            boolean openNow = WEEKDAYS.contains(now.getDayOfWeek()) && !time.isBefore(open) && time.isBefore(close);
            if (openNow) {
                return EnqueueDecision.allow();
            }
            return EnqueueDecision.defer(Duration.between(now, nextOpening(now, open)));
        };
    }

    /**
     * Allow within a daily {@code [start, end)} window in {@code zone} (wraps past midnight); else defer to start.
     *
     * @param zone the zone the window is read in
     * @param start when the window opens, inclusive
     * @param end when it closes, exclusive; earlier than {@code start} means the
     *     window runs past midnight
     * @return the gate
     */
    public static EnqueueGate timeWindow(ZoneId zone, LocalTime start, LocalTime end) {
        return context -> {
            ZonedDateTime now = ZonedDateTime.now(zone);
            LocalTime time = now.toLocalTime();
            boolean inWindow = start.isBefore(end)
                    ? !time.isBefore(start) && time.isBefore(end)
                    : !time.isBefore(start) || time.isBefore(end);
            if (inWindow) {
                return EnqueueDecision.allow();
            }
            ZonedDateTime target = now.toLocalDate().atTime(start).atZone(zone);
            if (!target.isAfter(now)) {
                target = target.plusDays(1);
            }
            return EnqueueDecision.defer(Duration.between(now, target));
        };
    }

    /**
     * Allow on the given days in {@code zone}; otherwise defer to the start of the next allowed day.
     *
     * @param zone the zone the day is read in
     * @param allowed the days to run on; none listed skips every enqueue, since no
     *     later day would be allowed either
     * @return the gate
     */
    public static EnqueueGate dayOfWeek(ZoneId zone, DayOfWeek... allowed) {
        Set<DayOfWeek> days =
                allowed.length == 0 ? EnumSet.noneOf(DayOfWeek.class) : EnumSet.copyOf(Arrays.asList(allowed));
        return context -> {
            ZonedDateTime now = ZonedDateTime.now(zone);
            if (days.contains(now.getDayOfWeek())) {
                return EnqueueDecision.allow();
            }
            LocalDate date = now.toLocalDate().plusDays(1);
            for (int i = 0; i < 7; i++) {
                if (days.contains(date.getDayOfWeek())) {
                    return EnqueueDecision.defer(Duration.between(now, date.atStartOfDay(zone)));
                }
                date = date.plusDays(1);
            }
            return EnqueueDecision.skip("no allowed day of week");
        };
    }

    /**
     * Allow only when {@code test} accepts the payload; otherwise skip silently.
     *
     * @param test run against the payload as passed, before serialization
     * @return the gate
     */
    public static EnqueueGate payloadMatches(java.util.function.Predicate<Object> test) {
        return context ->
                test.test(context.payload()) ? EnqueueDecision.allow() : EnqueueDecision.skip("payload did not match");
    }

    /**
     * Allow only when {@code flag} is enabled; otherwise skip silently.
     *
     * @param flag the flag's name
     * @param flags where to look it up; consulted on every enqueue, so it should be cheap
     * @return the gate
     */
    public static EnqueueGate featureFlag(String flag, BooleanFlag flags) {
        return context ->
                flags.enabled(flag) ? EnqueueDecision.allow() : EnqueueDecision.skip("feature '" + flag + "' disabled");
    }

    /** The next weekday opening at or after {@code now} (today at {@code open} if still ahead). */
    private static ZonedDateTime nextOpening(ZonedDateTime now, LocalTime open) {
        if (WEEKDAYS.contains(now.getDayOfWeek()) && now.toLocalTime().isBefore(open)) {
            return now.toLocalDate().atTime(open).atZone(now.getZone());
        }
        LocalDate date = now.toLocalDate().plusDays(1);
        for (int i = 0; i < 7; i++) {
            if (WEEKDAYS.contains(date.getDayOfWeek())) {
                return date.atTime(open).atZone(now.getZone());
            }
            date = date.plusDays(1);
        }
        return now;
    }
}
