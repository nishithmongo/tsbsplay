// tsbs_generate_queries generates queries for various use cases. Its output will
// be consumed by the corresponding tsbs_run_queries_ program.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/timescale/tsbs/pkg/query/config"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/finance"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/internal/inputs"
	internalUtils "github.com/timescale/tsbs/internal/utils"
)

var useCaseMatrix = map[string]map[string]utils.QueryFillerMaker{
	"devops": {
		devops.LabelSingleGroupby + "-1-1-1":  devops.NewSingleGroupby(1, 1, 1),
		devops.LabelSingleGroupby + "-1-1-12": devops.NewSingleGroupby(1, 1, 12),
		devops.LabelSingleGroupby + "-1-8-1":  devops.NewSingleGroupby(1, 8, 1),
		devops.LabelSingleGroupby + "-5-1-1":  devops.NewSingleGroupby(5, 1, 1),
		devops.LabelSingleGroupby + "-5-1-12": devops.NewSingleGroupby(5, 1, 12),
		devops.LabelSingleGroupby + "-5-8-1":  devops.NewSingleGroupby(5, 8, 1),
		devops.LabelMaxAll + "-1":             devops.NewMaxAllCPU(1, devops.MaxAllDuration),
		devops.LabelMaxAll + "-8":             devops.NewMaxAllCPU(8, devops.MaxAllDuration),
		devops.LabelMaxAll + "-32-24":         devops.NewMaxAllCPU(32, 24*time.Hour),
		devops.LabelDoubleGroupby + "-1":      devops.NewGroupBy(1),
		devops.LabelDoubleGroupby + "-5":      devops.NewGroupBy(5),
		devops.LabelDoubleGroupby + "-all":    devops.NewGroupBy(devops.GetCPUMetricsLen()),
		devops.LabelGroupbyOrderbyLimit:       devops.NewGroupByOrderByLimit,
		devops.LabelHighCPU + "-all":          devops.NewHighCPU(0),
		devops.LabelHighCPU + "-1":            devops.NewHighCPU(1),
		devops.LabelLastpoint:                 devops.NewLastPointPerHost,
	},
	"iot": {
		iot.LabelLastLoc:                       iot.NewLastLocPerTruck,
		iot.LabelLastLocSingleTruck:            iot.NewLastLocSingleTruck,
		iot.LabelLowFuel:                       iot.NewTruckWithLowFuel,
		iot.LabelHighLoad:                      iot.NewTruckWithHighLoad,
		iot.LabelStationaryTrucks:              iot.NewStationaryTrucks,
		iot.LabelLongDrivingSessions:           iot.NewTrucksWithLongDrivingSession,
		iot.LabelLongDailySessions:             iot.NewTruckWithLongDailySession,
		iot.LabelAvgVsProjectedFuelConsumption: iot.NewAvgVsProjectedFuelConsumption,
		iot.LabelAvgDailyDrivingDuration:       iot.NewAvgDailyDrivingDuration,
		iot.LabelAvgDailyDrivingSession:        iot.NewAvgDailyDrivingSession,
		iot.LabelAvgLoad:                       iot.NewAvgLoad,
		iot.LabelDailyActivity:                 iot.NewDailyTruckActivity,
		iot.LabelBreakdownFrequency:            iot.NewTruckBreakdownFrequency,
	},
	"finance": {
		finance.LabelLastPrice:                                finance.NewLastPrice,
		finance.LabelMovingAverage + "-1h-15m-10":             finance.NewMovingAverage(time.Hour, 15*time.Minute, 10),
		finance.LabelMovingAverage + "-1h-15m-20":             finance.NewMovingAverage(time.Hour, 15*time.Minute, 20),
		finance.LabelMovingAverage + "-1h-15m-50":             finance.NewMovingAverage(time.Hour, 15*time.Minute, 50),
		finance.LabelMovingAverage + "-1h-15m-100":            finance.NewMovingAverage(time.Hour, 15*time.Minute, 100),
		finance.LabelMovingAverage + "-1h-15m-200":            finance.NewMovingAverage(time.Hour, 15*time.Minute, 200),
		finance.LabelMovingAverage + "-4h-1h-10":              finance.NewMovingAverage(4*time.Hour, time.Hour, 10),
		finance.LabelMovingAverage + "-4h-1h-20":              finance.NewMovingAverage(4*time.Hour, time.Hour, 20),
		finance.LabelMovingAverage + "-4h-1h-50":              finance.NewMovingAverage(4*time.Hour, time.Hour, 50),
		finance.LabelMovingAverage + "-4h-1h-100":             finance.NewMovingAverage(4*time.Hour, time.Hour, 100),
		finance.LabelMovingAverage + "-4h-1h-200":             finance.NewMovingAverage(4*time.Hour, time.Hour, 200),
		finance.LabelMovingAverage + "-1d-4h-10":              finance.NewMovingAverage(24*time.Hour, 4*time.Hour, 10),
		finance.LabelMovingAverage + "-1d-4h-20":              finance.NewMovingAverage(24*time.Hour, 4*time.Hour, 20),
		finance.LabelMovingAverage + "-1d-4h-50":              finance.NewMovingAverage(24*time.Hour, 4*time.Hour, 50),
		finance.LabelMovingAverage + "-1d-4h-100":             finance.NewMovingAverage(24*time.Hour, 4*time.Hour, 100),
		finance.LabelMovingAverage + "-1d-4h-200":             finance.NewMovingAverage(24*time.Hour, 4*time.Hour, 200),
		finance.LabelMovingAverage + "-1w-1d-10":              finance.NewMovingAverage(7*24*time.Hour, 24*time.Hour, 10),
		finance.LabelMovingAverage + "-1w-1d-20":              finance.NewMovingAverage(7*24*time.Hour, 24*time.Hour, 20),
		finance.LabelMovingAverage + "-1w-1d-50":              finance.NewMovingAverage(7*24*time.Hour, 24*time.Hour, 50),
		finance.LabelMovingAverage + "-1w-1d-100":             finance.NewMovingAverage(7*24*time.Hour, 24*time.Hour, 100),
		finance.LabelMovingAverage + "-1w-1d-200":             finance.NewMovingAverage(7*24*time.Hour, 24*time.Hour, 200),
		finance.LabelExponentialMovingAverage + "-1h-15m-10":  finance.NewExponentialMovingAverage(time.Hour, 15*time.Minute, 10),
		finance.LabelExponentialMovingAverage + "-1h-15m-20":  finance.NewExponentialMovingAverage(time.Hour, 15*time.Minute, 20),
		finance.LabelExponentialMovingAverage + "-1h-15m-50":  finance.NewExponentialMovingAverage(time.Hour, 15*time.Minute, 50),
		finance.LabelExponentialMovingAverage + "-1h-15m-100": finance.NewExponentialMovingAverage(time.Hour, 15*time.Minute, 100),
		finance.LabelExponentialMovingAverage + "-1h-15m-200": finance.NewExponentialMovingAverage(time.Hour, 15*time.Minute, 200),
		finance.LabelExponentialMovingAverage + "-4h-1h-10":   finance.NewExponentialMovingAverage(4*time.Hour, time.Hour, 10),
		finance.LabelExponentialMovingAverage + "-4h-1h-20":   finance.NewExponentialMovingAverage(4*time.Hour, time.Hour, 20),
		finance.LabelExponentialMovingAverage + "-4h-1h-50":   finance.NewExponentialMovingAverage(4*time.Hour, time.Hour, 50),
		finance.LabelExponentialMovingAverage + "-4h-1h-100":  finance.NewExponentialMovingAverage(4*time.Hour, time.Hour, 100),
		finance.LabelExponentialMovingAverage + "-4h-1h-200":  finance.NewExponentialMovingAverage(4*time.Hour, time.Hour, 200),
		finance.LabelExponentialMovingAverage + "-1d-4h-10":   finance.NewExponentialMovingAverage(24*time.Hour, 4*time.Hour, 10),
		finance.LabelExponentialMovingAverage + "-1d-4h-20":   finance.NewExponentialMovingAverage(24*time.Hour, 4*time.Hour, 20),
		finance.LabelExponentialMovingAverage + "-1d-4h-50":   finance.NewExponentialMovingAverage(24*time.Hour, 4*time.Hour, 50),
		finance.LabelExponentialMovingAverage + "-1d-4h-100":  finance.NewExponentialMovingAverage(24*time.Hour, 4*time.Hour, 100),
		finance.LabelExponentialMovingAverage + "-1d-4h-200":  finance.NewExponentialMovingAverage(24*time.Hour, 4*time.Hour, 200),
		finance.LabelExponentialMovingAverage + "-1w-1d-10":   finance.NewExponentialMovingAverage(7*24*time.Hour, 24*time.Hour, 10),
		finance.LabelExponentialMovingAverage + "-1w-1d-20":   finance.NewExponentialMovingAverage(7*24*time.Hour, 24*time.Hour, 20),
		finance.LabelExponentialMovingAverage + "-1w-1d-50":   finance.NewExponentialMovingAverage(7*24*time.Hour, 24*time.Hour, 50),
		finance.LabelExponentialMovingAverage + "-1w-1d-100":  finance.NewExponentialMovingAverage(7*24*time.Hour, 24*time.Hour, 100),
		finance.LabelExponentialMovingAverage + "-1w-1d-200":  finance.NewExponentialMovingAverage(7*24*time.Hour, 24*time.Hour, 200),
		finance.LabelRSI + "-1h-15m-2":                        finance.NewRSI(time.Hour, 15*time.Minute, 2),
		finance.LabelRSI + "-1h-15m-6":                        finance.NewRSI(time.Hour, 15*time.Minute, 6),
		finance.LabelRSI + "-4h-1h-2":                         finance.NewRSI(4*time.Hour, time.Hour, 2),
		finance.LabelRSI + "-4h-1h-6":                         finance.NewRSI(4*time.Hour, time.Hour, 6),
		finance.LabelRSI + "-1d-4h-14":                        finance.NewRSI(24*time.Hour, 4*time.Hour, 14),
		finance.LabelRSI + "-1d-4h-20":                        finance.NewRSI(24*time.Hour, 4*time.Hour, 20),
		finance.LabelRSI + "-1w-1d-14":                        finance.NewRSI(7*24*time.Hour, 24*time.Hour, 14),
		finance.LabelRSI + "-1w-1d-20":                        finance.NewRSI(7*24*time.Hour, 24*time.Hour, 20),
		finance.LabelMACD + "-1h-15m-12-26-9":                 finance.NewMACD(time.Hour, 15*time.Minute, 12, 26, 9),
		finance.LabelMACD + "-4h-1h-12-26-9":                  finance.NewMACD(4*time.Hour, time.Hour, 12, 26, 9),
		finance.LabelMACD + "-1d-4h-12-26-9":                  finance.NewMACD(24*time.Hour, 4*time.Hour, 12, 26, 9),
		finance.LabelMACD + "-1w-1d-12-26-9":                  finance.NewMACD(7*24*time.Hour, 24*time.Hour, 12, 26, 9),
		finance.LabelMACD + "-1h-15m-19-26-9":                 finance.NewMACD(time.Hour, 15*time.Minute, 19, 26, 9),
		finance.LabelMACD + "-4h-1h-19-26-9":                  finance.NewMACD(4*time.Hour, time.Hour, 19, 26, 9),
		finance.LabelMACD + "-1d-4h-19-26-9":                  finance.NewMACD(24*time.Hour, 4*time.Hour, 19, 26, 9),
		finance.LabelMACD + "-1w-1d-19-26-9":                  finance.NewMACD(7*24*time.Hour, 24*time.Hour, 19, 26, 9),
		finance.LabelStochasticOscillator + "-1h-15m-5":       finance.NewStochasticOscillator(time.Hour, 15*time.Minute, 5),
		finance.LabelStochasticOscillator + "-4h-1h-5":        finance.NewStochasticOscillator(4*time.Hour, time.Hour, 5),
		finance.LabelStochasticOscillator + "-1d-4h-5":        finance.NewStochasticOscillator(24*time.Hour, 4*time.Hour, 5),
		finance.LabelStochasticOscillator + "-1w-1d-5":        finance.NewStochasticOscillator(7*24*time.Hour, 24*time.Hour, 5),
		finance.LabelStochasticOscillator + "-1h-15m-14":      finance.NewStochasticOscillator(time.Hour, 15*time.Minute, 14),
		finance.LabelStochasticOscillator + "-4h-1h-14":       finance.NewStochasticOscillator(4*time.Hour, time.Hour, 14),
		finance.LabelStochasticOscillator + "-1d-4h-14":       finance.NewStochasticOscillator(24*time.Hour, 4*time.Hour, 14),
		finance.LabelStochasticOscillator + "-1w-1d-14":       finance.NewStochasticOscillator(7*24*time.Hour, 24*time.Hour, 14),
		finance.LabelTopPercentChange + "-1h-15m":             finance.NewTopPercentChange(time.Hour, 15*time.Minute),
		finance.LabelTopPercentChange + "-4h-1h":              finance.NewTopPercentChange(24*time.Hour, time.Hour),
		finance.LabelTopPercentChange + "-1d-4h":              finance.NewTopPercentChange(24*time.Hour, 4*time.Hour),
		finance.LabelTopPercentChange + "-1w-1d":              finance.NewTopPercentChange(7*24*time.Hour, 24*time.Hour),
	},
}

var conf = &config.QueryGeneratorConfig{}

// Parse args:
func init() {
	useCaseMatrix["cpu-only"] = useCaseMatrix["devops"]
	// Change the Usage function to print the use case matrix of choices:
	oldUsage := pflag.Usage
	pflag.Usage = func() {
		oldUsage()

		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "The use case matrix of choices is:\n")
		for uc, queryTypes := range useCaseMatrix {
			for qt := range queryTypes {
				fmt.Fprintf(os.Stderr, "  use case: %s, query type: %s\n", uc, qt)
			}
		}
	}

	conf.AddToFlagSet(pflag.CommandLine)

	pflag.Parse()

	err := internalUtils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&conf.BaseConfig); err != nil {
		panic(fmt.Errorf("unable to decode base config: %s", err))
	}

	if err := viper.Unmarshal(&conf); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}
}

func main() {
	qg := inputs.NewQueryGenerator(useCaseMatrix)
	err := qg.Generate(conf)
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}
}
