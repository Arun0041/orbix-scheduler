package cron

import (
	"fmt"
	"time"
)

func (s *Schedule) Next(from time.Time) (time.Time, bool) {
	t := from.Truncate(time.Minute).Add(time.Minute)
	if s.hasSeconds {
		t = from.Truncate(time.Second).Add(time.Second)
	}
	limit := from.AddDate(4, 0, 0)
	for t.Before(limit) {
		if s.matches(t) {
			return t, true
		}
		if s.hasSeconds {
			if s.hours&(1<<uint(t.Hour())) == 0 {
				t = t.Truncate(time.Hour).Add(time.Hour)
				continue
			}
			if s.minutes&(1<<uint(t.Minute())) == 0 {
				t = t.Truncate(time.Minute).Add(time.Minute)
				continue
			}
			t = t.Add(time.Second)
		} else {
			t = t.Add(time.Minute)
		}
	}
	return time.Time{}, false
}

func (s *Schedule) matches(t time.Time) bool {
	if s.months&(1<<uint(int(t.Month()))) == 0 {
		return false
	}
	if s.hours&(1<<uint(t.Hour())) == 0 {
		return false
	}
	if s.minutes&(1<<uint(t.Minute())) == 0 {
		return false
	}
	if s.hasSeconds && s.seconds&(1<<uint(t.Second())) == 0 {
		return false
	}
	domOK := s.doms&(1<<uint(t.Day())) != 0
	dowOK := s.dows&(1<<uint(int(t.Weekday()))) != 0
	switch {
	case s.domStar && s.dowStar:
		return true
	case s.domStar:
		return dowOK
	case s.dowStar:
		return domOK
	default:
		return domOK || dowOK
	}
}

func NextAfter(expr, tz string, now time.Time) (time.Time, error) {
	s, err := Parse(expr)
	if err != nil {
		return time.Time{}, err
	}
	loc := time.UTC
	if tz != "" {
		l, err := time.LoadLocation(tz)
		if err != nil {
			return time.Time{}, fmt.Errorf("cron: unknown timezone %q", tz)
		}
		loc = l
	}
	nxt, ok := s.Next(now.In(loc))
	if !ok {
		return time.Time{}, fmt.Errorf("cron: %q has no fire time within 4 years", expr)
	}
	return nxt.UTC(), nil
}
