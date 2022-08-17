package finance

import (
	"testing"
	"time"

	"github.com/timescale/tsbs/pkg/data"
)

func TestPriceToPoint(t *testing.T) {
	now := time.Now()
	m := NewPrice(now, 1*time.Second)
	duration := time.Second
	m.Tick(duration)

	p := data.NewPoint()
	m.ToPoint(p)
	if got := string(p.MeasurementName()); got != "price" {
		t.Errorf("incorrect measurement name: got %s want 'price'", got)
	}

	if got := p.GetFieldValue([]byte("price")); got == nil {
		t.Errorf("field 'price' returned a nil value unexpectedly")
	}
}
