package cron

import (
	"testing"
	"time"
	_ "time/tzdata"
)

func at(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestNext(t *testing.T) {
	cases := []struct {
		name string
		expr string
		from string
		want string
	}{
		{"hourly at :00", "0 * * * *", "2026-01-15T10:20:30Z", "2026-01-15T11:00:00Z"},
		{"every 15 min", "*/15 * * * *", "2026-01-15T10:20:00Z", "2026-01-15T10:30:00Z"},
		{"step from offset", "5/15 * * * *", "2026-01-15T10:06:00Z", "2026-01-15T10:20:00Z"},
		{"list of minutes", "5,35 * * * *", "2026-01-15T10:06:00Z", "2026-01-15T10:35:00Z"},
		{"range of hours", "0 9-17 * * *", "2026-01-15T18:00:00Z", "2026-01-16T09:00:00Z"},
		{"step in range", "0 0-12/4 * * *", "2026-01-15T05:00:00Z", "2026-01-15T08:00:00Z"},
		{"noon daily", "0 12 * * *", "2026-01-15T10:06:00Z", "2026-01-15T12:00:00Z"},
		{"@daily macro", "@daily", "2026-01-15T00:00:00Z", "2026-01-16T00:00:00Z"},
		{"@hourly macro", "@hourly", "2026-01-15T10:59:59Z", "2026-01-15T11:00:00Z"},
		{"first of month", "0 0 1 * *", "2026-01-15T00:00:00Z", "2026-02-01T00:00:00Z"},
		{"sundays", "0 0 * * 0", "2026-01-15T00:00:00Z", "2026-01-18T00:00:00Z"},
		{"dow 7 is sunday", "0 0 * * 7", "2026-01-15T00:00:00Z", "2026-01-18T00:00:00Z"},
		{"weekday names", "0 9 * * mon-fri", "2026-01-17T10:00:00Z", "2026-01-19T09:00:00Z"},
		{"month names", "0 0 1 jan-mar *", "2026-04-01T00:00:00Z", "2027-01-01T00:00:00Z"},
		{"leap day", "0 0 29 2 *", "2026-03-01T00:00:00Z", "2028-02-29T00:00:00Z"},
		{"vixie OR: 13th or friday", "0 0 13 * 5", "2026-01-10T00:00:00Z", "2026-01-13T00:00:00Z"},
		{"dom only", "0 0 13 * *", "2026-01-10T00:00:00Z", "2026-01-13T00:00:00Z"},
		{"new year's eve", "59 23 31 12 *", "2026-01-01T00:00:00Z", "2026-12-31T23:59:00Z"},
		{"seconds every 10", "*/10 * * * * *", "2026-01-15T10:00:05Z", "2026-01-15T10:00:10Z"},
		{"seconds specific", "30 0 12 * * *", "2026-01-15T12:00:31Z", "2026-01-16T12:00:30Z"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := Parse(c.expr)
			if err != nil {
				t.Fatalf("Parse(%q): %v", c.expr, err)
			}
			got, ok := s.Next(at(c.from))
			if !ok {
				t.Fatalf("Next(%q) found no fire time", c.expr)
			}
			if want := at(c.want); !got.Equal(want) {
				t.Errorf("Next(%q) from %s = %s, want %s", c.expr, c.from, got.UTC(), want)
			}
		})
	}
}

func TestInvalid(t *testing.T) {
	bad := []string{
		"",
		"* * * *",          
		"60 * * * *",       
		"* 24 * * *",       
		"* * 32 * *",       
		"* * * 13 *",       
		"* * * * 8",        
		"a * * * *",        
		"@reboot",          
		"*/0 * * * *",      
		"1- * * * *",       
		"5-3 * * * *",      
		"1 2 3 4 5 6 7",   
		",5 * * * *",       
	}
	for _, e := range bad {
		if _, err := Parse(e); err == nil {
			t.Errorf("Parse(%q) should fail", e)
		}
	}
}

func TestNextAfterTimezone(t *testing.T) {
	got, err := NextAfter("0 9 * * *", "Asia/Kolkata", at("2026-01-15T03:00:00Z"))
	if err != nil {
		t.Fatalf("NextAfter: %v", err)
	}
	if want := at("2026-01-15T03:30:00Z"); !got.Equal(want) {
		t.Errorf("got %s, want %s", got.UTC(), want)
	}
}

func TestNextAfterBadTimezone(t *testing.T) {
	if _, err := NextAfter("0 9 * * *", "Mars/Olympus", time.Now()); err == nil {
		t.Errorf("unknown timezone should fail")
	}
}
