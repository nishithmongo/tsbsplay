package mongo

import (
	"encoding/gob"
	"fmt"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/finance"
	"github.com/timescale/tsbs/pkg/query"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func init() {
	gob.Register(bson.A{})
}

type Finance struct {
	*BaseGenerator
	*finance.Core
}

func idTimeSortStage() bson.D {
	return bson.D{
		{"$sort", bson.D{
			{"_id.time", -1},
		}},
	}
}

func hourDiffPipeline(end time.Time, span time.Duration) mongo.Pipeline {
	return mongo.Pipeline{
		{
			{"$match", bson.D{
				{"$expr", bson.D{
					{"$gte", bson.A{
						"$time",
						bson.D{
							{"$dateSubtract", bson.D{
								{"startDate", end},
								{"unit", "hour"},
								{"amount", span.Hours()},
							}},
						},
					}},
				}},
			}},
		},
	}
}

func sortOpenHighLowClosePipeline(interval time.Duration) mongo.Pipeline {
	return mongo.Pipeline{
		{
			{"$sort", bson.D{
				{"time", 1},
			}},
		},
		{
			{"$group", bson.D{
				{"_id", bson.D{
					{"symbol", "$tags.symbol"},
					{"time", bson.D{
						{"$dateTrunc", bson.D{
							{"date", "$time"},
							{"unit", "minute"},
							{"binSize", interval.Minutes()},
						}},
					}},
				}},
				{"high", bson.D{
					{"$max", "$price"},
				}},
				{"low", bson.D{
					{"$min", "$price"},
				}},
				{"open", bson.D{
					{"$first", "$price"},
				}},
				{"close", bson.D{
					{"$last", "$price"},
				}},
			}},
		},
	}
}

func hourDiffSortOpenHighLowClosePipeline(end time.Time, span, interval time.Duration) mongo.Pipeline {
	pipeline := mongo.Pipeline{}
	pipeline = append(pipeline, hourDiffPipeline(end, span)...)
	pipeline = append(pipeline, sortOpenHighLowClosePipeline(interval)...)
	return pipeline
}

func (f *Finance) LastPrice(q query.Query) {
	query := q.(*query.Mongo)
	query.Pipeline = mongo.Pipeline{
		{
			{"$sort", bson.D{
				{"time", -1},
			}},
		},
		{
			{"$group", bson.D{
				{"_id", "$tags.symbol"},
				{"lastPrice", bson.D{
					{"$first", "$price"},
				}},
			}},
		},
	}
	query.CollectionName = []byte("point_data")
	query.HumanLabel = []byte("MongoDB last price per symbol")
	query.HumanDescription = query.HumanLabel
}

func (f *Finance) MovingAverage(q query.Query, span, interval time.Duration, points int) {
	query := q.(*query.Mongo)
	query.Pipeline = append(query.Pipeline, hourDiffSortOpenHighLowClosePipeline(f.Core.Interval.End(), span, interval)...)
	query.Pipeline = append(query.Pipeline, mongo.Pipeline{
		{
			{"$setWindowFields", bson.D{
				{"partitionBy", "$_id.symbol"},
				{"sortBy", bson.D{
					{"_id.time", 1},
				}},
				{"output", bson.D{
					{"movingAverage", bson.D{
						{"$avg", "$close"},
						{"window", bson.D{
							{"documents", bson.A{
								(points * -1) + 1,
								0,
							}},
						}},
					}},
				}},
			}},
		},
	}...)
	query.Pipeline = append(query.Pipeline, idTimeSortStage())
	query.CollectionName = []byte("point_data")
	query.HumanLabel = []byte("MongoDB moving average")
	query.HumanDescription = []byte(fmt.Sprintf("%s, last %s, interval %s, %d previous data points",
		query.HumanLabel,
		span,
		interval,
		points))
}

func (f *Finance) ExponentialMovingAverage(q query.Query, span, interval time.Duration, points int) {
	query := q.(*query.Mongo)
	query.Pipeline = append(query.Pipeline, hourDiffSortOpenHighLowClosePipeline(f.Core.Interval.End(), span, interval)...)
	query.Pipeline = append(query.Pipeline, mongo.Pipeline{
		{
			{"$setWindowFields", bson.D{
				{"partitionBy", "$_id.symbol"},
				{"sortBy", bson.D{
					{"_id.time", 1},
				}},
				{"output", bson.D{
					{"expMovingAverage", bson.D{
						{"$expMovingAvg", bson.D{
							{"input", "$close"},
							{"N", points},
						}},
					}},
				}},
			}},
		},
	}...)
	query.Pipeline = append(query.Pipeline, idTimeSortStage())
	query.CollectionName = []byte("point_data")
	query.HumanLabel = []byte("MongoDB exponential moving average")
	query.HumanDescription = []byte(fmt.Sprintf("%s, last %s, interval %s, %d previous data points",
		query.HumanLabel,
		span,
		interval,
		points))
}

func (f *Finance) RSI(q query.Query, span, interval time.Duration, points int) {
	query := q.(*query.Mongo)
	query.Pipeline = append(query.Pipeline, hourDiffSortOpenHighLowClosePipeline(f.Core.Interval.End(), span, interval)...)
	query.Pipeline = append(query.Pipeline, mongo.Pipeline{
		{
			{"$setWindowFields", bson.D{
				{"partitionBy", "$_id.symbol"},
				{"sortBy", bson.D{
					{"_id.time", 1},
				}},
				{"output", bson.D{
					{"prevClose", bson.D{
						{"$shift", bson.D{
							{"by", -1},
							{"output", "$close"},
						}},
					}},
				}},
			}},
		},
		{
			{"$addFields", bson.D{
				{"diff", bson.D{
					{"$subtract", bson.A{
						"$close",
						bson.D{
							{"$ifNull", bson.A{
								"$prevClose",
								"$close",
							}},
						},
					}},
				}},
			}},
		},
		{
			{"$addFields", bson.D{
				{"gain", bson.D{
					{"$cond", bson.D{
						{"if", bson.D{
							{"$gte", bson.A{
								"$diff",
								0,
							}},
						}},
						{"then", "$diff"},
						{"else", 0},
					}},
				}},
				{"loss", bson.D{
					{"$cond", bson.D{
						{"if", bson.D{
							{"$lte", bson.A{
								"$diff",
								0,
							}},
						}},
						{"then", bson.D{
							{"$abs", "$diff"},
						}},
						{"else", 0},
					}},
				}},
			}},
		},
		{
			{"$setWindowFields", bson.D{
				{"partitionBy", "$_id.symbol"},
				{"sortBy", bson.D{
					{"_id.time", 1},
				}},
				{"output", bson.D{
					{"avgGain", bson.D{
						{"$avg", "$gain"},
						{"window", bson.D{
							{"documents", bson.A{
								(points * -1) + 1,
								0,
							}},
						}},
					}},
					{"avgLoss", bson.D{
						{"$avg", "$loss"},
						{"window", bson.D{
							{"documents", bson.A{
								(points * -1) + 1,
								0,
							}},
						}},
					}},
					{"rankNo", bson.D{
						{"$rank", bson.M{}},
					}},
				}},
			}},
		},
		{
			{"$addFields", bson.D{
				{"relativeStrength", bson.D{
					{"$cond", bson.D{
						{"if", bson.D{
							{"$gt", bson.A{
								"$avgLoss",
								0,
							}},
						}},
						{"then", bson.D{
							{"$divide", bson.A{
								"$avgGain",
								"$avgLoss",
							}},
						}},
						{"else", "$avgGain"},
					}},
				}},
			}},
		},
		{
			{"$addFields", bson.D{
				{"rsi", bson.D{
					{"$cond", bson.D{
						{"if", bson.D{
							{"$gt", bson.A{
								"$rankNo",
								points,
							}},
						}},
						{"then", bson.D{
							{"$subtract", bson.A{
								100,
								bson.D{
									{"$divide", bson.A{
										100,
										bson.D{
											{"$add", bson.A{
												1,
												"$relativeStrength",
											}},
										},
									}},
								},
							}},
						}},
						{"else", nil},
					}},
				}},
			}},
		},
	}...)
	query.Pipeline = append(query.Pipeline, idTimeSortStage())
	query.CollectionName = []byte("point_data")
	query.HumanLabel = []byte("MongoDB relative strength index")
	query.HumanDescription = []byte(fmt.Sprintf("%s, last %s, interval %s, %d previous data points",
		query.HumanLabel,
		span,
		interval,
		points))
}

func (f *Finance) MACD(q query.Query, span, interval time.Duration, firstPoints, secondPoints, signalPoints int) {
	query := q.(*query.Mongo)
	query.Pipeline = append(query.Pipeline, hourDiffSortOpenHighLowClosePipeline(f.Core.Interval.End(), span, interval)...)
	query.Pipeline = append(query.Pipeline, mongo.Pipeline{
		{
			{"$setWindowFields", bson.D{
				{"partitionBy", "$_id.symbol"},
				{"sortBy", bson.D{
					{"_id.time", 1},
				}},
				{"output", bson.D{
					{"macd01", bson.D{
						{"$expMovingAvg", bson.D{
							{"input", "$close"},
							{"N", firstPoints},
						}},
					}},
					{"macd02", bson.D{
						{"$expMovingAvg", bson.D{
							{"input", "$close"},
							{"N", secondPoints},
						}},
					}},
				}},
			}},
		},
		{
			{"$addFields", bson.D{
				{"macdLine", bson.D{
					{"$subtract", bson.A{
						"$macd01",
						"$macd02",
					}},
				}},
			}},
		},
		{
			{"$setWindowFields", bson.D{
				{"partitionBy", "$_id.symbol"},
				{"sortBy", bson.D{
					{"_id.time", 1},
				}},
				{"output", bson.D{
					{"macdSignal", bson.D{
						{"$expMovingAvg", bson.D{
							{"input", "$macdLine"},
							{"N", signalPoints},
						}},
					}},
				}},
			}},
		},
		{
			{"$addFields", bson.D{
				{"macdHistogram", bson.D{
					{"$subtract", bson.A{
						"$macdLine",
						"$macdSignal",
					}},
				}},
			}},
		},
	}...)
	query.Pipeline = append(query.Pipeline, idTimeSortStage())
	query.CollectionName = []byte("point_data")
	query.HumanLabel = []byte("MongoDB moving average convergence/divergence")
	query.HumanDescription = []byte(fmt.Sprintf("%s, last %s, interval %s, (%d, %d, %d) previous data points",
		query.HumanLabel,
		span,
		interval,
		firstPoints,
		secondPoints,
		signalPoints))
}

func (f *Finance) StochasticOscillator(q query.Query, span, interval time.Duration, points int) {
	query := q.(*query.Mongo)
	query.Pipeline = append(query.Pipeline, hourDiffSortOpenHighLowClosePipeline(f.Core.Interval.End(), span, interval)...)
	query.Pipeline = append(query.Pipeline, mongo.Pipeline{
		{
			{"$setWindowFields", bson.D{
				{"partitionBy", "$_id.symbol"},
				{"sortBy", bson.D{
					{"_id.time", 1},
				}},
				{"output", bson.D{
					{"stocOsscHighest", bson.D{
						{"$max", "$high"},
						{"window", bson.D{
							{"documents", bson.A{
								(points * -1) + 1,
								0,
							}},
						}},
					}},
					{"stocOsscLowest", bson.D{
						{"$min", "$low"},
						{"window", bson.D{
							{"documents", bson.A{
								(points * -1) + 1,
								0,
							}},
						}},
					}},
					{"documentNumber", bson.D{
						{"$documentNumber", bson.M{}},
					}},
				}},
			}},
		},
		{
			{"$addFields", bson.D{
				{"stocOsscKValue", bson.D{
					{"$cond", bson.D{
						{"if", bson.D{
							{"$gt", bson.A{
								"$documentNumber",
								points,
							}},
						}},
						{"then", bson.D{
							{"$round", bson.A{
								bson.D{
									{"$multiply", bson.A{
										bson.D{
											{"$divide", bson.A{
												bson.D{
													{"$subtract", bson.A{
														"$close",
														"$stocOsscLowest",
													}},
												},
												bson.D{
													{"$subtract", bson.A{
														"$stocOsscHighest",
														"$stocOsscLowest",
													}},
												},
											}},
										},
										100,
									}},
								},
								2,
							}},
						}},
						{"else", nil},
					}},
				}},
			}},
		},
		{
			{"$setWindowFields", bson.D{
				{"partitionBy", "$_id.symbol"},
				{"sortBy", bson.D{
					{"_id.time", 1},
				}},
				{"output", bson.D{
					{"stocOsscDValue", bson.D{
						{"$avg", "$stocOsscKValue"},
						{"window", bson.D{
							{"documents", bson.A{
								-2,
								0,
							}},
						}},
					}},
				}},
			}},
		},
		{
			{"$set", bson.D{
				{"stocOsscKValue", bson.D{
					{"$round", bson.A{
						"$stocOsscKValue",
						2,
					}},
				}},
				{"stocOsscDValue", bson.D{
					{"$round", bson.A{
						"$stocOsscDValue",
						2,
					}},
				}},
			}},
		},
	}...)
	query.Pipeline = append(query.Pipeline, idTimeSortStage())
	query.CollectionName = []byte("point_data")
	query.HumanLabel = []byte("MongoDB stochastic oscillator")
	query.HumanDescription = []byte(fmt.Sprintf("%s, last %s, interval %s, %d previous data points", query.HumanLabel, span, interval, points))
}

func (f *Finance) TopPercentChange(q query.Query, span, interval time.Duration) {
	query := q.(*query.Mongo)
	query.Pipeline = append(query.Pipeline, hourDiffPipeline(f.Core.Interval.End(), span)...)
	query.Pipeline = append(query.Pipeline, mongo.Pipeline{
		{
			{"$sort", bson.D{
				{"time", 1},
			}},
		},
		{
			{"$group", bson.D{
				{"_id", bson.D{
					{"symbol", "$tags.symbol"},
					{"time", bson.D{
						{"$dateTrunc", bson.D{
							{"date", "$time"},
							{"unit", "minute"},
							{"binSize", interval.Minutes()},
						}},
					}},
				}},
				{"open", bson.D{
					{"$first", "$price"},
				}},
				{"close", bson.D{
					{"$last", "$price"},
				}},
			}},
		},
		{
			{"$addFields", bson.D{
				{"diffPercentage", bson.D{
					{"$round", bson.A{
						bson.D{
							{"$multiply", bson.A{
								100,
								bson.D{
									{"$divide", bson.A{
										bson.D{
											{"$subtract", bson.A{
												"$close",
												"$open",
											}},
										},
										"$open",
									}},
								},
							}},
						},
						2,
					}},
				}},
			}},
		},
		{
			{"$group", bson.D{
				{"_id", "$_id.time"},
				{"topN", bson.D{
					{"$topN", bson.D{
						{"output", bson.A{
							"$_id.symbol",
							"$diffPercentage",
							"$close",
						}},
						{"sortBy", bson.D{
							{"diffPercentage", -1},
						}},
						{"n", 3},
					}},
				}},
				{"bottomN", bson.D{
					{"$bottomN", bson.D{
						{"output", bson.A{
							"$_id.symbol",
							"$diffPercentage",
							"$close",
						}},
						{"sortBy", bson.D{
							{"diffPercentage", -1},
						}},
						{"n", 3},
					}},
				}},
			}},
		},
		{
			{"$sort", bson.D{
				{"_id", -1},
			}},
		},
	}...)
	query.CollectionName = []byte("point_data")
	query.HumanLabel = []byte("MongoDB top percent change")
	query.HumanDescription = []byte(fmt.Sprintf("%s, last %s, interval %s",
		query.HumanLabel,
		span,
		interval))
}
