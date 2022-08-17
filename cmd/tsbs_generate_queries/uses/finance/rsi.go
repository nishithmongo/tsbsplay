package finance

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type RSI struct {
	core     utils.QueryGenerator
	span     time.Duration
	interval time.Duration
	points   int
}

func NewRSI(span, interval time.Duration, points int) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &RSI{
			core, span, interval, points,
		}
	}
}

func (d *RSI) Fill(q query.Query) query.Query {
	fc, ok := d.core.(RSIFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.RSI(q, d.span, d.interval, d.points)
	return q
}
