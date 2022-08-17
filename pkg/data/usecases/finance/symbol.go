package finance

import (
	"math/rand"
	"time"

	"github.com/timescale/tsbs/pkg/data/usecases/common"
)

func RandString(size int) string {
	const letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	s := make([]byte, size)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

type Symbol struct {
	simulatedMeasurements []common.SimulatedMeasurement
	tags                  []common.Tag
}

func (t *Symbol) TickAll(d time.Duration) {
	for i := range t.simulatedMeasurements {
		t.simulatedMeasurements[i].Tick(d)
	}
}

func (t Symbol) Measurements() []common.SimulatedMeasurement {
	return t.simulatedMeasurements
}

func (t Symbol) Tags() []common.Tag {
	return t.tags
}

func newSymbolPrice(start time.Time, interval time.Duration) []common.SimulatedMeasurement {
	return []common.SimulatedMeasurement{
		NewPrice(start, interval),
	}
}

func NewSymbol(i int, start time.Time, interval time.Duration) common.Generator {
	symbol := newSymbolWithPriceGenerator(i, start, interval, newSymbolPrice)
	return &symbol
}

func newSymbolWithPriceGenerator(i int, start time.Time, interval time.Duration, generator func(time.Time, time.Duration) []common.SimulatedMeasurement) Symbol {
	sm := generator(start, interval)

	measurement := Symbol{
		tags: []common.Tag{
			{Key: []byte("symbol"), Value: RandString(6)},
		},
		simulatedMeasurements: sm,
	}

	return measurement
}
