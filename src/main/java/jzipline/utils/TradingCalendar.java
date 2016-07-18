/*
# Copyright 2013 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
*/

package jzipline.utils;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.chrono.ChronoLocalDate;
import java.time.temporal.TemporalAdjuster;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.tuple.Pair;

//import pandas as pd
//from functools import partial

public class TradingCalendar {
    private static final int FRIDAY_VALUE = DayOfWeek.FRIDAY.getValue();
    private static final int THURSDAY_VALUE = DayOfWeek.THURSDAY.getValue();
    private static final int MONDAY_VALUE = DayOfWeek.MONDAY.getValue();
    private static final TemporalAdjuster THIRD_MONDAY_TEMP_ADJ = TemporalAdjusters.dayOfWeekInMonth( 3, DayOfWeek.MONDAY );
    private static final TemporalAdjuster FOURTH_THURSDAY_TEMP_ADJ = TemporalAdjusters.dayOfWeekInMonth( 4, DayOfWeek.THURSDAY );
    private static final TemporalAdjuster FIRST_MONDAY_TEMP_ADJ = TemporalAdjusters.firstInMonth( DayOfWeek.MONDAY );
    private static final TemporalAdjuster LAST_MONDAY_TEMP_ADJ = TemporalAdjusters.lastInMonth( DayOfWeek.MONDAY );
    
    private static final LocalDate START = LocalDate.of( 1990, 1, 1 );
//    # Give an aggressive buffer for logic that needs to use the next trading
//    # day or minute.
    private static final LocalDate END = LocalDate.now().plusDays( 365 );

    public static final SortedSet<LocalDate> NON_TRADING_DAYS = getNonTradingDays( START, END );
    public static final SortedSet<LocalDate> TRADING_DAYS = getTradingDays( START, END );
    public static final SortedSet<LocalDate> EARLY_CLOSES = getEarlyCloses( START, END );
    public static final Pair<Iterable<ZonedDateTime>,Iterable<ZonedDateTime>> OPEN_AND_CLOSES = getOpenAndCloses( TRADING_DAYS, EARLY_CLOSES );

//    def canonicalize_datetime(dt):
//        # Strip out any HHMMSS or timezone info in the user's datetime, so that
//        # all the datetimes we return will be 00:00:00 UTC.
//        return datetime(dt.year, dt.month, dt.day, tzinfo=pytz.utc)
    
    public static SortedSet<LocalDate> getNonTradingDays( LocalDate start, LocalDate end ) {
//        start = canonicalize_datetime(start)
//        end = canonicalize_datetime(end)
        final List<DateRule> nonTradingRules = new ArrayList<>();
        
        nonTradingRules.add( d -> {
           final DayOfWeek dow = d.getDayOfWeek();
           return dow == DayOfWeek.SATURDAY || dow == DayOfWeek.SUNDAY;
        });
        nonTradingRules.add( d -> d.getDayOfYear() == 1 );
        nonTradingRules.add( d -> d.getDayOfYear() == 2 && d.getDayOfWeek() == DayOfWeek.MONDAY );
    
        final DateRule mlkDayRule = d -> d.getYear() >= 1998 && d.getMonth() == Month.JANUARY && 
                d.getDayOfWeek() == DayOfWeek.MONDAY &&
                d.isEqual( (ChronoLocalDate)THIRD_MONDAY_TEMP_ADJ.adjustInto( d ) );
        nonTradingRules.add( mlkDayRule );
    
        final DateRule presidentsDayRule = d -> d.getMonth() == Month.FEBRUARY && 
                d.getDayOfWeek() == DayOfWeek.MONDAY &&
                d.isEqual( (ChronoLocalDate)THIRD_MONDAY_TEMP_ADJ.adjustInto( d ) );
        nonTradingRules.add( presidentsDayRule );
    
        final DateRule goodFridayRule = d -> d.getDayOfYear() == getEasterMonday( d.getYear() ) - 3;
        nonTradingRules.add( goodFridayRule );
    
        final DateRule memorialDayRule = d -> d.getMonth() == Month.MAY && 
                d.getDayOfWeek() == DayOfWeek.MONDAY &&
                d.isEqual( (ChronoLocalDate)LAST_MONDAY_TEMP_ADJ.adjustInto( d ) );
        nonTradingRules.add( memorialDayRule );
    
        final DateRule julyFourthRule = d -> { 
            final int dayOfMonth = d.getDayOfMonth();
            return d.getMonth() == Month.JULY && 
                    (dayOfMonth == 4 || 
                    (dayOfMonth == 5 && d.getDayOfWeek() == DayOfWeek.MONDAY) || 
                    (dayOfMonth == 3 && d.getDayOfWeek() == DayOfWeek.FRIDAY));
        };
        nonTradingRules.add( julyFourthRule );
    
        final DateRule laborDayRule = d -> d.getMonth() == Month.SEPTEMBER && 
                d.getDayOfWeek() == DayOfWeek.MONDAY && 
                d.isEqual( (ChronoLocalDate)FIRST_MONDAY_TEMP_ADJ.adjustInto( d ) );
        nonTradingRules.add( laborDayRule );
    
        final DateRule thanksgivingRule = d -> d.getMonth() == Month.NOVEMBER &&
                d.getDayOfWeek() == DayOfWeek.THURSDAY &&
                d.isEqual( (ChronoLocalDate)FOURTH_THURSDAY_TEMP_ADJ.adjustInto( d ) );
        nonTradingRules.add( thanksgivingRule );
    
        final DateRule christmasRule = d -> { 
            final int dayOfMonth = d.getDayOfMonth();
            return d.getMonth() == Month.DECEMBER &&
                (dayOfMonth == 25 ||
                (dayOfMonth == 26 && d.getDayOfWeek() == DayOfWeek.MONDAY) ||
                (dayOfMonth == 24 && d.getDayOfWeek() == DayOfWeek.FRIDAY));
        };
        nonTradingRules.add( christmasRule );
    
        final DateRule nonTradingRuleset = d -> nonTradingRules.stream().anyMatch( r -> r.ruleMatchesDate( d ) );
    
        final SortedSet<LocalDate> nonTradingDays = streamDates( start, end )
                .filter( nonTradingRuleset::ruleMatchesDate )
                .collect( Collectors.toCollection( TreeSet::new ) );
    
//        # Add September 11th closings
//        # http://en.wikipedia.org/wiki/Aftermath_of_the_September_11_attacks
//        # Due to the terrorist attacks, the stock market did not open on 9/11/2001
//        # It did not open again until 9/17/2001.
//        #
//        #    September 2001
//        # Su Mo Tu We Th Fr Sa
//        #                    1
//        #  2  3  4  5  6  7  8
//        #  9 10 11 12 13 14 15
//        # 16 17 18 19 20 21 22
//        # 23 24 25 26 27 28 29
//        # 30
        IntStream.rangeClosed( 11, 17 ).forEach( i -> nonTradingDays.add( LocalDate.of( 2001, 9, i ) ) );
    
//        # Add closings due to Hurricane Sandy in 2012
//        # http://en.wikipedia.org/wiki/Hurricane_sandy
//        #
//        # The stock exchange was closed due to Hurricane Sandy's
//        # impact on New York.
//        # It closed on 10/29 and 10/30, reopening on 10/31
//        #     October 2012
//        # Su Mo Tu We Th Fr Sa
//        #     1  2  3  4  5  6
//        #  7  8  9 10 11 12 13
//        # 14 15 16 17 18 19 20
//        # 21 22 23 24 25 26 27
//        # 28 29 30 31
        IntStream.rangeClosed( 29, 31 ).forEach( i -> nonTradingDays.add( LocalDate.of( 2012, 10, i ) ) );
    
//        # Misc closings from NYSE listing.
//        # http://www.nyse.com/pdfs/closings.pdf
//        #
//        # National Days of Mourning
//        # - President Richard Nixon
        nonTradingDays.add( LocalDate.of( 1994, 4, 27 ) );
//        # - President Ronald W. Reagan - June 11, 2004
        nonTradingDays.add( LocalDate.of( 2004, 6, 11 ) );
//        # - President Gerald R. Ford - Jan 2, 2007
        nonTradingDays.add( LocalDate.of( 2007, 1, 2 ) );
    
        return nonTradingDays;
    }

    private static Stream<LocalDate> streamDates( final LocalDate start, final LocalDate end ) {
        return StreamSupport.stream( 
                Spliterators.spliteratorUnknownSize( 
                        getCalendarDaysIterator( start, end ), 
                        Spliterator.DISTINCT & Spliterator.SORTED & Spliterator.ORDERED & Spliterator.NONNULL & Spliterator.IMMUTABLE ), 
                true );
    }
    
    public static SortedSet<LocalDate> getTradingDays( LocalDate start, LocalDate end ) {
        return getTradingDays( start, end, TradingCalendar.NON_TRADING_DAYS );
    }
    
    public static SortedSet<LocalDate> getTradingDays( LocalDate start, LocalDate end, Set<LocalDate> nonTradingDays ) {
        return streamDates( start, end )
                .filter( d -> !nonTradingDays.contains( d ) )
                .collect( Collectors.toCollection( TreeSet::new ) );
    }
    
    public static SortedSet<LocalDate> getEarlyCloses( LocalDate start, LocalDate end ) {
//        # 1:00 PM close rules based on
//        # http://quant.stackexchange.com/questions/4083/nyse-early-close-rules-july-4th-and-dec-25th # noqa
//        # and verified against http://www.nyse.com/pdfs/closings.pdf
//    
//        # These rules are valid starting in 1993
    
//        start = canonicalize_datetime(start)
//        end = canonicalize_datetime(end)
        final LocalDate earlyCloseRulesStartDate = LocalDate.of( 1993, 1, 1 );
        if( earlyCloseRulesStartDate.isAfter( start ) )
            start = earlyCloseRulesStartDate;
        
        if( earlyCloseRulesStartDate.isAfter( end ) )
            end = earlyCloseRulesStartDate;

        
//        # Not included here are early closes prior to 1993
//        # or unplanned early closes
    
        final List<DateRule> earlyCloseRules = new ArrayList<>();
    
        final DateRule dayAfterThanksgivingRule = d -> {
            final int dayOfMonth = d.getDayOfMonth();
            return d.getMonth() == Month.NOVEMBER && d.getDayOfWeek() == DayOfWeek.FRIDAY && dayOfMonth >= 23 && dayOfMonth <= 30;
        };
        earlyCloseRules.add( dayAfterThanksgivingRule );
    
        final DateRule christmasEveRule = d -> {
            final int dowVal = d.getDayOfWeek().getValue();
            return d.getMonth() == Month.DECEMBER && 
                    ( (d.getDayOfMonth() == 24 && dowVal >= MONDAY_VALUE && dowVal <= THURSDAY_VALUE) ||
                                //            # valid 1993-2007
                            (d.getDayOfMonth() == 26 && dowVal == FRIDAY_VALUE && d.getYear() <= 2007) );
        };
        earlyCloseRules.add( christmasEveRule );
    
        final DateRule independenceDayRule = d -> {
            final DayOfWeek dayOfWeek = d.getDayOfWeek();
            return d.getMonth() == Month.JULY && 
                    ((d.getDayOfMonth() == 3 && (dayOfWeek == DayOfWeek.MONDAY || dayOfWeek == DayOfWeek.TUESDAY || dayOfWeek == DayOfWeek.THURSDAY)) ||
                            (d.getYear() < 2013 ? d.getDayOfMonth() == 5 && dayOfWeek == DayOfWeek.FRIDAY : d.getDayOfMonth() == 3 && dayOfWeek == DayOfWeek.WEDNESDAY));
        };
        earlyCloseRules.add( independenceDayRule );
        
        final DateRule earlyCloseRuleset = d -> earlyCloseRules.stream().anyMatch( r -> r.ruleMatchesDate( d ) );
    
        final SortedSet<LocalDate> earlyCloses = streamDates( start, end )
                .filter( earlyCloseRuleset::ruleMatchesDate )
                .collect( Collectors.toCollection( TreeSet::new ) );
    
//        # Misc early closings from NYSE listing.
//        # http://www.nyse.com/pdfs/closings.pdf
//        #
//        # New Year's Eve
        final LocalDate nye1999 = LocalDate.of( 1999, 12, 31 );
        if( !start.isAfter( nye1999 ) && !end.isBefore( nye1999 ) )
            earlyCloses.add( nye1999 );
//    
        return earlyCloses;
    }
    
    private static Pair<ZonedDateTime,ZonedDateTime> getOpenAndClose( final LocalDate day, final Set<LocalDate> earlyCloses ) {
        final ZonedDateTime marketOpen = ZonedDateTime.of( day, LocalTime.of( 9, 31 ), ZoneId.of( "America/New_York" ) ).withZoneSameInstant( ZoneId.of( "UTC" ) );

        // # 1 PM if early close, 4 PM otherwise
        final int closeHour = earlyCloses.contains( day ) ? 13 : 16;
        final ZonedDateTime marketClose = ZonedDateTime.of( day, LocalTime.of( closeHour, 0 ), ZoneId.of( "America/New_York" ) ).withZoneSameInstant( ZoneId.of( "UTC" ) );
    
        return Pair.of( marketOpen, marketClose );
    }
    
    public static Pair<Iterable<ZonedDateTime>,Iterable<ZonedDateTime>> getOpenAndCloses( final Iterable<LocalDate> tradingDays, final Set<LocalDate> earlyCloses ) {
        final List<ZonedDateTime> opens = new ArrayList<>();
        final List<ZonedDateTime> closes = new ArrayList<>();

        StreamSupport.stream( tradingDays.spliterator(), false ).forEachOrdered( d -> {
            final Pair<ZonedDateTime,ZonedDateTime> oc = getOpenAndClose( d, earlyCloses );
            opens.add( oc.getLeft() );
            closes.add( oc.getRight() );
        } );
    
        return Pair.of( opens, closes );
    }
    
    public static Iterator<LocalDate> getCalendarDaysIterator( final LocalDate start, final LocalDate end ) {
        return new CalendarDaysIterator( start, end );
    }
    
    public static Iterable<LocalDate> datesBetween( final LocalDate start, final LocalDate end ) {
        return new Iterable<LocalDate>() {
            @Override
            public Iterator<LocalDate> iterator() {
                return TradingCalendar.getCalendarDaysIterator( start, end );
            }

        };
    }
    
    private static final class CalendarDaysIterator implements Iterator<LocalDate> {
//        private final LocalDate start;
        private final LocalDate end;
        
        private LocalDate next;

        private CalendarDaysIterator( LocalDate start, LocalDate end ) {
//            this.start = start;
            next = start;
            this.end = end;
        }

        @Override
        public boolean hasNext() {
            return !next.isAfter( end );
        }

        @Override
        public LocalDate next() {
            if( next.isAfter( end ) )
                throw new NoSuchElementException();
            
            LocalDate ret = next;
            next = next.plusDays( 1 );
            return ret;
        }
    }

    public interface DateRule {
        public boolean ruleMatchesDate( LocalDate date );
    }
    
    private static final short EASTER_MONDAY[] = {
            98, 90, 103, 95, 114, 106, 91, 111, 102, // 1901-1909
            87, 107, 99, 83, 103, 95, 115, 99, 91, 111, // 1910-1919
            96, 87, 107, 92, 112, 103, 95, 108, 100, 91, // 1920-1929
            111, 96, 88, 107, 92, 112, 104, 88, 108, 100, // 1930-1939
            85, 104, 96, 116, 101, 92, 112, 97, 89, 108, // 1940-1949
            100, 85, 105, 96, 109, 101, 93, 112, 97, 89, // 1950-1959
            109, 93, 113, 105, 90, 109, 101, 86, 106, 97, // 1960-1969
            89, 102, 94, 113, 105, 90, 110, 101, 86, 106, // 1970-1979
            98, 110, 102, 94, 114, 98, 90, 110, 95, 86, // 1980-1989
            106, 91, 111, 102, 94, 107, 99, 90, 103, 95, // 1990-1999
            115, 106, 91, 111, 103, 87, 107, 99, 84, 103, // 2000-2009
            95, 115, 100, 91, 111, 96, 88, 107, 92, 112, // 2010-2019
            104, 95, 108, 100, 92, 111, 96, 88, 108, 92, // 2020-2029
            112, 104, 89, 108, 100, 85, 105, 96, 116, 101, // 2030-2039
            93, 112, 97, 89, 109, 100, 85, 105, 97, 109, // 2040-2049
            101, 93, 113, 97, 89, 109, 94, 113, 105, 90, // 2050-2059
            110, 101, 86, 106, 98, 89, 102, 94, 114, 105, // 2060-2069
            90, 110, 102, 86, 106, 98, 111, 102, 94, 114, // 2070-2079
            99, 90, 110, 95, 87, 106, 91, 111, 103, 94, // 2080-2089
            107, 99, 91, 103, 95, 115, 107, 91, 111, 103, // 2090-2099
            88, 108, 100, 85, 105, 96, 109, 101, 93, 112, // 2100-2109
            97, 89, 109, 93, 113, 105, 90, 109, 101, 86, // 2110-2119
            106, 97, 89, 102, 94, 113, 105, 90, 110, 101, // 2120-2129
            86, 106, 98, 110, 102, 94, 114, 98, 90, 110, // 2130-2139
            95, 86, 106, 91, 111, 102, 94, 107, 99, 90, // 2140-2149
            103, 95, 115, 106, 91, 111, 103, 87, 107, 99, // 2150-2159
            84, 103, 95, 115, 100, 91, 111, 96, 88, 107, // 2160-2169
            92, 112, 104, 95, 108, 100, 92, 111, 96, 88, // 2170-2179
            108, 92, 112, 104, 89, 108, 100, 85, 105, 96, // 2180-2189
            116, 101, 93, 112, 97, 89, 109, 100, 85, 105 // 2190-2199
    };

    /**
     * expressed relative to first day of year
     * 
     * @param y
     *        - year
     * @return
     */
    private static int getEasterMonday( final int year ) {
        return EASTER_MONDAY[year - 1901];
    }
}


/*
import pandas as pd
import pytz

from datetime import datetime
from dateutil import rrule
from functools import partial

start = pd.Timestamp('1990-01-01', tz='UTC')
end_base = pd.Timestamp('today', tz='UTC')
# Give an aggressive buffer for logic that needs to use the next trading
# day or minute.
end = end_base + pd.Timedelta(days=365)


def canonicalize_datetime(dt):
    # Strip out any HHMMSS or timezone info in the user's datetime, so that
    # all the datetimes we return will be 00:00:00 UTC.
    return datetime(dt.year, dt.month, dt.day, tzinfo=pytz.utc)


def get_non_trading_days(start, end):
    non_trading_rules = []

    start = canonicalize_datetime(start)
    end = canonicalize_datetime(end)

    weekends = rrule.rrule(
        rrule.YEARLY,
        byweekday=(rrule.SA, rrule.SU),
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(weekends)

    new_years = rrule.rrule(
        rrule.MONTHLY,
        byyearday=1,
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(new_years)

    new_years_sunday = rrule.rrule(
        rrule.MONTHLY,
        byyearday=2,
        byweekday=rrule.MO,
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(new_years_sunday)

    mlk_day = rrule.rrule(
        rrule.MONTHLY,
        bymonth=1,
        byweekday=(rrule.MO(+3)),
        cache=True,
        dtstart=datetime(1998, 1, 1, tzinfo=pytz.utc),
        until=end
    )
    non_trading_rules.append(mlk_day)

    presidents_day = rrule.rrule(
        rrule.MONTHLY,
        bymonth=2,
        byweekday=(rrule.MO(3)),
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(presidents_day)

    good_friday = rrule.rrule(
        rrule.DAILY,
        byeaster=-2,
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(good_friday)

    memorial_day = rrule.rrule(
        rrule.MONTHLY,
        bymonth=5,
        byweekday=(rrule.MO(-1)),
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(memorial_day)

    july_4th = rrule.rrule(
        rrule.MONTHLY,
        bymonth=7,
        bymonthday=4,
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(july_4th)

    july_4th_sunday = rrule.rrule(
        rrule.MONTHLY,
        bymonth=7,
        bymonthday=5,
        byweekday=rrule.MO,
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(july_4th_sunday)

    july_4th_saturday #
# Copyright 2013 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import pandas as pd

start = pd.Timestamp('1990-01-01', tz='UTC')
end_base = pd.Timestamp('today', tz='UTC')
# Give an aggressive buffer for logic that needs to use the next trading
# day or minute.
end = end_base + pd.Timedelta(days=365)


def canonicalize_datetime(dt):
    # Strip out any HHMMSS or timezone info in the user's datetime, so that
    # all the datetimes we return will be 00:00:00 UTC.
    return datetime(dt.year, dt.month, dt.day, tzinfo=pytz.utc)


def get_non_trading_days(start, end):
    non_trading_rules = []

    start = canonicalize_datetime(start)
    end = canonicalize_datetime(end)

    weekends = rrule.rrule(
        rrule.YEARLY,
        byweekday=(rrule.SA, rrule.SU),
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(weekends)

    new_years = rrule.rrule(
        rrule.MONTHLY,
        byyearday=1,
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(new_years)

    new_years_sunday = rrule.rrule(
        rrule.MONTHLY,
        byyearday=2,
        byweekday=rrule.MO,
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(new_years_sunday)

    mlk_day = rrule.rrule(
        rrule.MONTHLY,
        bymonth=1,
        byweekday=(rrule.MO(+3)),
        cache=True,
        dtstart=datetime(1998, 1, 1, tzinfo=pytz.utc),
        until=end
    )
    non_trading_rules.append(mlk_day)

    presidents_day = rrule.rrule(
        rrule.MONTHLY,
        bymonth=2,
        byweekday=(rrule.MO(3)),
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(presidents_day)

    good_friday = rrule.rrule(
        rrule.DAILY,
        byeaster=-2,
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(good_friday)

    memorial_day = rrule.rrule(
        rrule.MONTHLY,
        bymonth=5,
        byweekday=(rrule.MO(-1)),
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(memorial_day)

    july_4th = rrule.rrule(
        rrule.MONTHLY,
        bymonth=7,
        bymonthday=4,
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(july_4th)

    july_4th_sunday = rrule.rrule(
        rrule.MONTHLY,
        bymonth=7,
        bymonthday=5,
        byweekday=rrule.MO,
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(july_4th_sunday)

    july_4th_saturday = rrule.rrule(
        rrule.MONTHLY,
        bymonth=7,
        bymonthday=3,
        byweekday=rrule.FR,
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(july_4th_saturday)

    labor_day = rrule.rrule(
        rrule.MONTHLY,
        bymonth=9,
        byweekday=(rrule.MO(1)),
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(labor_day)

    thanksgiving = rrule.rrule(
        rrule.MONTHLY,
        bymonth=11,
        byweekday=(rrule.TH(4)),
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(thanksgiving)

    christmas = rrule.rrule(
        rrule.MONTHLY,
        bymonth=12,
        bymonthday=25,
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(christmas)

    christmas_sunday = rrule.rrule(
        rrule.MONTHLY,
        bymonth=12,
        bymonthday=26,
        byweekday=rrule.MO,
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(christmas_sunday)

    # If Christmas is a Saturday then 24th, a Friday is observed.
    christmas_saturday = rrule.rrule(
        rrule.MONTHLY,
        bymonth=12,
        bymonthday=24,
        byweekday=rrule.FR,
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(christmas_saturday)

    non_trading_ruleset = rrule.rruleset()

    for rule in non_trading_rules:
        non_trading_ruleset.rrule(rule)

    non_trading_days = non_trading_ruleset.between(start, end, inc=True)

    # Add September 11th closings
    # http://en.wikipedia.org/wiki/Aftermath_of_the_September_11_attacks
    # Due to the terrorist attacks, the stock market did not open on 9/11/2001
    # It did not open again until 9/17/2001.
    #
    #    September 2001
    # Su Mo Tu We Th Fr Sa
    #                    1
    #  2  3  4  5  6  7  8
    #  9 10 11 12 13 14 15
    # 16 17 18 19 20 21 22
    # 23 24 25 26 27 28 29
    # 30

    for day_num in range(11, 17):
        non_trading_days.append(
            datetime(2001, 9, day_num, tzinfo=pytz.utc))

    # Add closings due to Hurricane Sandy in 2012
    # http://en.wikipedia.org/wiki/Hurricane_sandy
    #
    # The stock exchange was closed due to Hurricane Sandy's
    # impact on New York.
    # It closed on 10/29 and 10/30, reopening on 10/31
    #     October 2012
    # Su Mo Tu We Th Fr Sa
    #     1  2  3  4  5  6
    #  7  8  9 10 11 12 13
    # 14 15 16 17 18 19 20
    # 21 22 23 24 25 26 27
    # 28 29 30 31

    for day_num in range(29, 31):
        non_trading_days.append(
            datetime(2012, 10, day_num, tzinfo=pytz.utc))

    # Misc closings from NYSE listing.
    # http://www.nyse.com/pdfs/closings.pdf
    #
    # National Days of Mourning
    # - President Richard Nixon
    non_trading_days.append(datetime(1994, 4, 27, tzinfo=pytz.utc))
    # - President Ronald W. Reagan - June 11, 2004
    non_trading_days.append(datetime(2004, 6, 11, tzinfo=pytz.utc))
    # - President Gerald R. Ford - Jan 2, 2007
    non_trading_days.append(datetime(2007, 1, 2, tzinfo=pytz.utc))

    non_trading_days.sort()
    return pd.DatetimeIndex(non_trading_days)

non_trading_days = get_non_trading_days(start, end)
trading_day = pd.tseries.offsets.CDay(holidays=non_trading_days)


def get_trading_days(start, end, trading_day=trading_day):
    return pd.date_range(start=start.date(),
                         end=end.date(),
                         freq=trading_day).tz_localize('UTC')

trading_days = get_trading_days(start, end)


def get_early_closes(start, end):
    # 1:00 PM close rules based on
    # http://quant.stackexchange.com/questions/4083/nyse-early-close-rules-july-4th-and-dec-25th # noqa
    # and verified against http://www.nyse.com/pdfs/closings.pdf

    # These rules are valid starting in 1993

    start = canonicalize_datetime(start)
    end = canonicalize_datetime(end)

    start = max(start, datetime(1993, 1, 1, tzinfo=pytz.utc))
    end = max(end, datetime(1993, 1, 1, tzinfo=pytz.utc))

    # Not included here are early closes prior to 1993
    # or unplanned early closes

    early_close_rules = []

    day_after_thanksgiving = rrule.rrule(
        rrule.MONTHLY,
        bymonth=11,
        # 4th Friday isn't correct if month starts on Friday, so restrict to
        # day range:
        byweekday=(rrule.FR),
        bymonthday=range(23, 30),
        cache=True,
        dtstart=start,
        until=end
    )
    early_close_rules.append(day_after_thanksgiving)

    christmas_eve = rrule.rrule(
        rrule.MONTHLY,
        bymonth=12,
        bymonthday=24,
        byweekday=(rrule.MO, rrule.TU, rrule.WE, rrule.TH),
        cache=True,
        dtstart=start,
        until=end
    )
    early_close_rules.append(christmas_eve)

    friday_after_christmas = rrule.rrule(
        rrule.MONTHLY,
        bymonth=12,
        bymonthday=26,
        byweekday=rrule.FR,
        cache=True,
        dtstart=start,
        # valid 1993-2007
        until=min(end, datetime(2007, 12, 31, tzinfo=pytz.utc))
    )
    early_close_rules.append(friday_after_christmas)

    day_before_independence_day = rrule.rrule(
        rrule.MONTHLY,
        bymonth=7,
        bymonthday=3,
        byweekday=(rrule.MO, rrule.TU, rrule.TH),
        cache=True,
        dtstart=start,
        until=end
    )
    early_close_rules.append(day_before_independence_day)

    day_after_independence_day = rrule.rrule(
        rrule.MONTHLY,
        bymonth=7,
        bymonthday=5,
        byweekday=rrule.FR,
        cache=True,
        dtstart=start,
        # starting in 2013: wednesday before independence day
        until=min(end, datetime(2012, 12, 31, tzinfo=pytz.utc))
    )
    early_close_rules.append(day_after_independence_day)

    wednesday_before_independence_day = rrule.rrule(
        rrule.MONTHLY,
        bymonth=7,
        bymonthday=3,
        byweekday=rrule.WE,
        cache=True,
        # starting in 2013
        dtstart=max(start, datetime(2013, 1, 1, tzinfo=pytz.utc)),
        until=max(end, datetime(2013, 1, 1, tzinfo=pytz.utc))
    )
    early_close_rules.append(wednesday_before_independence_day)

    early_close_ruleset = rrule.rruleset()

    for rule in early_close_rules:
        early_close_ruleset.rrule(rule)
    early_closes = early_close_ruleset.between(start, end, inc=True)

    # Misc early closings from NYSE listing.
    # http://www.nyse.com/pdfs/closings.pdf
    #
    # New Year's Eve
    nye_1999 = datetime(1999, 12, 31, tzinfo=pytz.utc)
    if start <= nye_1999 and nye_1999 <= end:
        early_closes.append(nye_1999)

    early_closes.sort()
    return pd.DatetimeIndex(early_closes)

early_closes = get_early_closes(start, end)


def get_open_and_close(day, early_closes):
    market_open = pd.Timestamp(
        datetime(
            year=day.year,
            month=day.month,
            day=day.day,
            hour=9,
            minute=31),
        tz='US/Eastern').tz_convert('UTC')
    # 1 PM if early close, 4 PM otherwise
    close_hour = 13 if day in early_closes else 16
    market_close = pd.Timestamp(
        datetime(
            year=day.year,
            month=day.month,
            day=day.day,
            hour=close_hour),
        tz='US/Eastern').tz_convert('UTC')

    return market_open, market_close


def get_open_and_closes(trading_days, early_closes, get_open_and_close):
    open_and_closes = pd.DataFrame(index=trading_days,
                                   columns=('market_open', 'market_close'))

    get_o_and_c = partial(get_open_and_close, early_closes=early_closes)

    open_and_closes['market_open'], open_and_closes['market_close'] = \
        zip(*open_and_closes.index.map(get_o_and_c))

    return open_and_closes

open_and_closes = get_open_and_closes(trading_days, early_closes,
                                      get_open_and_close)
= rrule.rrule(
        rrule.MONTHLY,
        bymonth=7,
        bymonthday=3,
        byweekday=rrule.FR,
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(july_4th_saturday)

    labor_day = rrule.rrule(
        rrule.MONTHLY,
        bymonth=9,
        byweekday=(rrule.MO(1)),
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(labor_day)

    thanksgiving = rrule.rrule(
        rrule.MONTHLY,
        bymonth=11,
        byweekday=(rrule.TH(4)),
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(thanksgiving)

    christmas = rrule.rrule(
        rrule.MONTHLY,
        bymonth=12,
        bymonthday=25,
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(christmas)

    christmas_sunday = rrule.rrule(
        rrule.MONTHLY,
        bymonth=12,
        bymonthday=26,
        byweekday=rrule.MO,
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(christmas_sunday)

    # If Christmas is a Saturday then 24th, a Friday is observed.
    christmas_saturday = rrule.rrule(
        rrule.MONTHLY,
        bymonth=12,
        bymonthday=24,
        byweekday=rrule.FR,
        cache=True,
        dtstart=start,
        until=end
    )
    non_trading_rules.append(christmas_saturday)

    non_trading_ruleset = rrule.rruleset()

    for rule in non_trading_rules:
        non_trading_ruleset.rrule(rule)

    non_trading_days = non_trading_ruleset.between(start, end, inc=True)

    # Add September 11th closings
    # http://en.wikipedia.org/wiki/Aftermath_of_the_September_11_attacks
    # Due to the terrorist attacks, the stock market did not open on 9/11/2001
    # It did not open again until 9/17/2001.
    #
    #    September 2001
    # Su Mo Tu We Th Fr Sa
    #                    1
    #  2  3  4  5  6  7  8
    #  9 10 11 12 13 14 15
    # 16 17 18 19 20 21 22
    # 23 24 25 26 27 28 29
    # 30

    for day_num in range(11, 17):
        non_trading_days.append(
            datetime(2001, 9, day_num, tzinfo=pytz.utc))

    # Add closings due to Hurricane Sandy in 2012
    # http://en.wikipedia.org/wiki/Hurricane_sandy
    #
    # The stock exchange was closed due to Hurricane Sandy's
    # impact on New York.
    # It closed on 10/29 and 10/30, reopening on 10/31
    #     October 2012
    # Su Mo Tu We Th Fr Sa
    #     1  2  3  4  5  6
    #  7  8  9 10 11 12 13
    # 14 15 16 17 18 19 20
    # 21 22 23 24 25 26 27
    # 28 29 30 31

    for day_num in range(29, 31):
        non_trading_days.append(
            datetime(2012, 10, day_num, tzinfo=pytz.utc))

    # Misc closings from NYSE listing.
    # http://www.nyse.com/pdfs/closings.pdf
    #
    # National Days of Mourning
    # - President Richard Nixon
    non_trading_days.append(datetime(1994, 4, 27, tzinfo=pytz.utc))
    # - President Ronald W. Reagan - June 11, 2004
    non_trading_days.append(datetime(2004, 6, 11, tzinfo=pytz.utc))
    # - President Gerald R. Ford - Jan 2, 2007
    non_trading_days.append(datetime(2007, 1, 2, tzinfo=pytz.utc))

    non_trading_days.sort()
    return pd.DatetimeIndex(non_trading_days)

non_trading_days = get_non_trading_days(start, end)
trading_day = pd.tseries.offsets.CDay(holidays=non_trading_days)


def get_trading_days(start, end, trading_day=trading_day):
    return pd.date_range(start=start.date(),
                         end=end.date(),
                         freq=trading_day).tz_localize('UTC')

trading_days = get_trading_days(start, end)


def get_early_closes(start, end):
    # 1:00 PM close rules based on
    # http://quant.stackexchange.com/questions/4083/nyse-early-close-rules-july-4th-and-dec-25th # noqa
    # and verified against http://www.nyse.com/pdfs/closings.pdf

    # These rules are valid starting in 1993

    start = canonicalize_datetime(start)
    end = canonicalize_datetime(end)

    start = max(start, datetime(1993, 1, 1, tzinfo=pytz.utc))
    end = max(end, datetime(1993, 1, 1, tzinfo=pytz.utc))

    # Not included here are early closes prior to 1993
    # or unplanned early closes

    early_close_rules = []

    day_after_thanksgiving = rrule.rrule(
        rrule.MONTHLY,
        bymonth=11,
        # 4th Friday isn't correct if month starts on Friday, so restrict to
        # day range:
        byweekday=(rrule.FR),
        bymonthday=range(23, 30),
        cache=True,
        dtstart=start,
        until=end
    )
    early_close_rules.append(day_after_thanksgiving)

    christmas_eve = rrule.rrule(
        rrule.MONTHLY,
        bymonth=12,
        bymonthday=24,
        byweekday=(rrule.MO, rrule.TU, rrule.WE, rrule.TH),
        cache=True,
        dtstart=start,
        until=end
    )
    early_close_rules.append(christmas_eve)

    friday_after_christmas = rrule.rrule(
        rrule.MONTHLY,
        bymonth=12,
        bymonthday=26,
        byweekday=rrule.FR,
        cache=True,
        dtstart=start,
        # valid 1993-2007
        until=min(end, datetime(2007, 12, 31, tzinfo=pytz.utc))
    )
    early_close_rules.append(friday_after_christmas)

    day_before_independence_day = rrule.rrule(
        rrule.MONTHLY,
        bymonth=7,
        bymonthday=3,
        byweekday=(rrule.MO, rrule.TU, rrule.TH),
        cache=True,
        dtstart=start,
        until=end
    )
    early_close_rules.append(day_before_independence_day)

    day_after_independence_day = rrule.rrule(
        rrule.MONTHLY,
        bymonth=7,
        bymonthday=5,
        byweekday=rrule.FR,
        cache=True,
        dtstart=start,
        # starting in 2013: wednesday before independence day
        until=min(end, datetime(2012, 12, 31, tzinfo=pytz.utc))
    )
    early_close_rules.append(day_after_independence_day)

    wednesday_before_independence_day = rrule.rrule(
        rrule.MONTHLY,
        bymonth=7,
        bymonthday=3,
        byweekday=rrule.WE,
        cache=True,
        # starting in 2013
        dtstart=max(start, datetime(2013, 1, 1, tzinfo=pytz.utc)),
        until=max(end, datetime(2013, 1, 1, tzinfo=pytz.utc))
    )
    early_close_rules.append(wednesday_before_independence_day)

    early_close_ruleset = rrule.rruleset()

    for rule in early_close_rules:
        early_close_ruleset.rrule(rule)
    early_closes = early_close_ruleset.between(start, end, inc=True)

    # Misc early closings from NYSE listing.
    # http://www.nyse.com/pdfs/closings.pdf
    #
    # New Year's Eve
    nye_1999 = datetime(1999, 12, 31, tzinfo=pytz.utc)
    if start <= nye_1999 and nye_1999 <= end:
        early_closes.append(nye_1999)

    early_closes.sort()
    return pd.DatetimeIndex(early_closes)

early_closes = get_early_closes(start, end)


def get_open_and_close(day, early_closes):
    market_open = pd.Timestamp(
        datetime(
            year=day.year,
            month=day.month,
            day=day.day,
            hour=9,
            minute=31),
        tz='US/Eastern').tz_convert('UTC')
    # 1 PM if early close, 4 PM otherwise
    close_hour = 13 if day in early_closes else 16
    market_close = pd.Timestamp(
        datetime(
            year=day.year,
            month=day.month,
            day=day.day,
            hour=close_hour),
        tz='US/Eastern').tz_convert('UTC')

    return market_open, market_close


def get_open_and_closes(trading_days, early_closes, get_open_and_close):
    open_and_closes = pd.DataFrame(index=trading_days,
                                   columns=('market_open', 'market_close'))

    get_o_and_c = partial(get_open_and_close, early_closes=early_closes)

    open_and_closes['market_open'], open_and_closes['market_close'] = \
        zip(*open_and_closes.index.map(get_o_and_c))

    return open_and_closes

open_and_closes = get_open_and_closes(trading_days, early_closes,
                                      get_open_and_close)
*/