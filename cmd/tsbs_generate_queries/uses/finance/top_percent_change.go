package finance

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type TopPercentChange struct {
	core     utils.QueryGenerator
	span     time.Duration
	interval time.Duration
}

func NewTopPercentChange(span, interval time.Duration) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &TopPercentChange{
			core, span, interval,
		}
	}
}

func (d *TopPercentChange) Fill(q query.Query) query.Query {
	fc, ok := d.core.(TopPercentChangeFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.TopPercentChange(q, d.span, d.interval)
	return q
}
