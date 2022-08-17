package finance

import (
	"testing"
	"time"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
)

func testGenerator(start time.Time, interval time.Duration) []common.SimulatedMeasurement {
	return []common.SimulatedMeasurement{
		&testMeasurement{ticks: 0},
	}
}

type testMeasurement struct {
	ticks int
}

func (m *testMeasurement) Tick(_ time.Duration)  { m.ticks++ }
func (m *testMeasurement) ToPoint(_ *data.Point) {}

func TestNewSymbolPrice(t *testing.T) {
	start := time.Now()

	measurements := newSymbolPrice(start, 1*time.Second)

	if got := len(measurements); got != 1 {
		t.Errorf("incorrect number of measurements: got %d want %d", got, 2)
	}

	readings := measurements[0].(*Price)
	if got := readings.Timestamp; got != start {
		t.Errorf("incorrect readings measurement timestamp: got %v want %v", got, start)
	}
}

func TestNewSymbol(t *testing.T) {
	start := time.Now()
	generator := NewSymbol(1, start, 1*time.Second)

	symbol := generator.(*Symbol)

	if got := len(symbol.Measurements()); got != 1 {
		t.Errorf("incorrect truck measurement count: got %v want %v", got, 2)
	}

	if got := len(symbol.Tags()); got != 1 {
		t.Errorf("incorrect truck tag count: got %v want %v", got, 8)
	}
}

func TestSymbolTickAll(t *testing.T) {
	now := time.Now()
	symbol := newSymbolWithPriceGenerator(0, now, 1*time.Second, testGenerator)
	if got := symbol.simulatedMeasurements[0].(*testMeasurement).ticks; got != 0 {
		t.Errorf("ticks not equal to 0 to start: got %d", got)
	}
	symbol.TickAll(time.Second)
	if got := symbol.simulatedMeasurements[0].(*testMeasurement).ticks; got != 1 {
		t.Errorf("ticks incorrect: got %d want %d", got, 1)
	}
	symbol.simulatedMeasurements = append(symbol.simulatedMeasurements, &testMeasurement{})
	symbol.TickAll(time.Second)
	if got := symbol.simulatedMeasurements[0].(*testMeasurement).ticks; got != 2 {
		t.Errorf("ticks incorrect after 2nd tick: got %d want %d", got, 2)
	}
	if got := symbol.simulatedMeasurements[1].(*testMeasurement).ticks; got != 1 {
		t.Errorf("ticks incorrect after 2nd tick: got %d want %d", got, 1)
	}
}
