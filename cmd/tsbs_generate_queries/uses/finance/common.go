package finance

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/pkg/query"
)

const (
	LabelLastPrice                = "last-price"
	LabelMovingAverage            = "moving-average"
	LabelExponentialMovingAverage = "exponential-moving-average"
	LabelRSI                      = "rsi"
	LabelMACD                     = "macd"
	LabelStochasticOscillator     = "stochastic-oscillator"
	LabelTopPercentChange         = "top-percent-change"
)

type Core struct {
	*common.Core
}

func NewCore(start, end time.Time, scale int) (*Core, error) {
	c, err := common.NewCore(start, end, scale)
	return &Core{Core: c}, err
}

type LastPriceFiller interface {
	LastPrice(query.Query)
}

type MovingAverageFiller interface {
	MovingAverage(query.Query, time.Duration, time.Duration, int)
}

type ExponentialMovingAverageFiller interface {
	ExponentialMovingAverage(query.Query, time.Duration, time.Duration, int)
}

type RSIFiller interface {
	RSI(query.Query, time.Duration, time.Duration, int)
}

type MACDFiller interface {
	MACD(query.Query, time.Duration, time.Duration, int, int, int)
}

type StochasticOscillatorFiller interface {
	StochasticOscillator(query.Query, time.Duration, time.Duration, int)
}

type TopPercentChangeFiller interface {
	TopPercentChange(query.Query, time.Duration, time.Duration)
}
