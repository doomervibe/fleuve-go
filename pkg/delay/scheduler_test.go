package delay

import (
	"testing"
	"time"
)

func TestNextCronFireEmpty(t *testing.T) {
	if NextCronFire("", "") != nil {
		t.Fatal("expected nil")
	}
}

func TestNextCronFireInvalidExpr(t *testing.T) {
	if NextCronFire("not-a-cron", "") != nil {
		t.Fatal("expected nil for invalid expression")
	}
}

func TestNextCronFireReturnsFuture(t *testing.T) {
	n := NextCronFire("*/5 * * * *", "")
	if n == nil {
		t.Fatal("nil")
	}
	if !n.After(time.Now().Add(-time.Minute)) {
		t.Fatalf("expected future-ish time: %v", n)
	}
}

func TestNextCronFiresCount(t *testing.T) {
	fires := NextCronFires("0 * * * *", "", 3)
	if len(fires) != 3 {
		t.Fatalf("len=%d", len(fires))
	}
	for i := 1; i < len(fires); i++ {
		if !fires[i].After(fires[i-1]) {
			t.Fatalf("not increasing: %v %v", fires[i-1], fires[i])
		}
	}
}

func TestNextCronFiresEmptyExpr(t *testing.T) {
	if NextCronFires("", "", 5) != nil {
		t.Fatal("expected nil")
	}
}
