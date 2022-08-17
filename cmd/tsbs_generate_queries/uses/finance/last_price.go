package finance

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type LastPrice struct {
	core utils.QueryGenerator
}

func NewLastPrice(core utils.QueryGenerator) utils.QueryFiller {
	return &LastPrice{core}
}

func (d *LastPrice) Fill(q query.Query) query.Query {
	fc, ok := d.core.(LastPriceFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.LastPrice(q)
	return q
}
