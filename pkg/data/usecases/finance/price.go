package finance

import (
	"math/rand"
	"time"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
)

type Price struct {
	*common.SubsystemMeasurement
}

func (price *Price) ToPoint(point *data.Point) {
	point.SetMeasurementName([]byte("price"))
	copy := price.Timestamp
	point.SetTimestamp(&copy)

	for _, d := range price.Distributions {
		point.AppendField([]byte("price"), float64(d.Get()))
	}
}

func NewPrice(start time.Time, interval time.Duration) *Price {
	return &Price{
		SubsystemMeasurement: common.NewSubsystemMeasurementWithDistributionMakers(start,
			[]common.LabeledDistributionMaker{
				{
					Label: []byte("price"),
					DistributionMaker: func() common.Distribution {
						return common.FP(
							common.CWD(common.ND(0, interval.Seconds()/100), 0.0, 100.0, rand.Float64()*100),
							5,
						)
					},
				},
			}),
	}
}
