package finance

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type MACD struct {
	core         utils.QueryGenerator
	span         time.Duration
	interval     time.Duration
	firstPoints  int
	secondPoints int
	signalPoints int
}

func NewMACD(span, interval time.Duration, firstPoints, secondPoints, signalPoints int) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &MACD{
			core, span, interval, firstPoints, secondPoints, signalPoints,
		}
	}
}

func (d *MACD) Fill(q query.Query) query.Query {
	fc, ok := d.core.(MACDFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.MACD(q, d.span, d.interval, d.firstPoints, d.secondPoints, d.signalPoints)
	return q
}
