package finance

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type ExponentialMovingAverage struct {
	core     utils.QueryGenerator
	span     time.Duration
	interval time.Duration
	points   int
}

func NewExponentialMovingAverage(span, interval time.Duration, points int) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &MovingAverage{
			core, span, interval, points,
		}
	}
}

func (d *ExponentialMovingAverage) Fill(q query.Query) query.Query {
	fc, ok := d.core.(ExponentialMovingAverageFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.ExponentialMovingAverage(q, d.span, d.interval, d.points)
	return q
}
