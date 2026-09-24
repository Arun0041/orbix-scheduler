package cron

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	secMin, secMax   = 0, 59
	minMin, minMax   = 0, 59
	hourMin, hourMax = 0, 23
	domMin, domMax   = 1, 31
	monMin, monMax   = 1, 12
	dowMin, dowMax   = 0, 7
)

type Schedule struct {
	seconds    uint64
	minutes    uint64
	hours      uint64
	doms       uint64
	months     uint64
	dows       uint64
	domStar    bool
	dowStar    bool
	hasSeconds bool
}

var monthNames = map[string]int{
	"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
	"jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

var dowNames = map[string]int{
	"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
}

func Validate(expr string) error {
	_, err := Parse(expr)
	return err
}

func Parse(expr string) (*Schedule, error) {
	e := strings.TrimSpace(strings.ToLower(expr))
	if e == "" {
		return nil, fmt.Errorf("cron: empty expression")
	}
	switch e {
	case "@yearly", "@annually":
		e = "0 0 1 1 *"
	case "@monthly":
		e = "0 0 1 * *"
	case "@weekly":
		e = "0 0 * * 0"
	case "@daily", "@midnight":
		e = "0 0 * * *"
	case "@hourly":
		e = "0 * * * *"
	}
	if strings.HasPrefix(e, "@") {
		return nil, fmt.Errorf("cron: unsupported macro %q", expr)
	}
	fields := strings.Fields(e)
	if len(fields) != 5 && len(fields) != 6 {
		return nil, fmt.Errorf("cron: expected 5 fields (min hour dom mon dow) or 6 (sec min hour dom mon dow), got %d in %q", len(fields), expr)
	}
	s := &Schedule{}
	i := 0
	var err error
	if len(fields) == 6 {
		if s.seconds, err = parseField(fields[0], secMin, secMax, 0, nil, "seconds"); err != nil {
			return nil, err
		}
		s.hasSeconds = true
		i = 1
	}
	if s.minutes, err = parseField(fields[i], minMin, minMax, 0, nil, "minute"); err != nil {
		return nil, err
	}
	if s.hours, err = parseField(fields[i+1], hourMin, hourMax, 0, nil, "hour"); err != nil {
		return nil, err
	}
	if s.doms, err = parseField(fields[i+2], domMin, domMax, 0, nil, "day-of-month"); err != nil {
		return nil, err
	}
	s.domStar = strings.HasPrefix(fields[i+2], "*")
	if s.months, err = parseField(fields[i+3], monMin, monMax, 0, monthNames, "month"); err != nil {
		return nil, err
	}
	if s.dows, err = parseField(fields[i+4], dowMin, dowMax, 7, dowNames, "day-of-week"); err != nil {
		return nil, err
	}
	s.dowStar = strings.HasPrefix(fields[i+4], "*")
	return s, nil
}

func parseField(field string, lo, hi, wrap int, names map[string]int, what string) (uint64, error) {
	var bits uint64
	for _, part := range strings.Split(field, ",") {
		if part == "" {
			return 0, fmt.Errorf("cron: empty item in %s field", what)
		}
		step := 1
		rng := part
		stepped := false
		if idx := strings.Index(part, "/"); idx >= 0 {
			rng = part[:idx]
			v, err := strconv.Atoi(part[idx+1:])
			if err != nil || v < 1 {
				return 0, fmt.Errorf("cron: invalid step in %s field: %q", what, part)
			}
			step = v
			stepped = true
		}
		from, to := lo, hi
		if rng != "*" {
			a, b := rng, ""
			isRange := false
			if idx := strings.Index(rng, "-"); idx >= 0 {
				a, b = rng[:idx], rng[idx+1:]
				isRange = true
			} else {
				b = a
			}
			av, err := parseValue(a, names, what)
			if err != nil {
				return 0, err
			}
			bv, err := parseValue(b, names, what)
			if err != nil {
				return 0, err
			}
			if av < lo || av > hi || bv < lo || bv > hi || av > bv {
				return 0, fmt.Errorf("cron: range out of bounds in %s field: %q", what, part)
			}
			from, to = av, bv
			if stepped && !isRange {
				to = hi
			}
		}
		for v := from; v <= to; v += step {
			bit := v
			if wrap > 0 {
				bit %= wrap
			}
			bits |= 1 << uint(bit)
		}
	}
	if bits == 0 {
		return 0, fmt.Errorf("cron: %s field matches nothing", what)
	}
	return bits, nil
}

func parseValue(s string, names map[string]int, what string) (int, error) {
	if s == "" {
		return 0, fmt.Errorf("cron: missing value in %s field", what)
	}
	if names != nil {
		if v, ok := names[s]; ok {
			return v, nil
		}
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("cron: bad value %q in %s field", s, what)
	}
	return v, nil
}
