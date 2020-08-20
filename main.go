package main

import (
	"context"
	"flag"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	thanosblock "github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"io/ioutil"
	"os"
)

func main() {
	var minTime, maxTime model.TimeOrDurationValue

	bktFile := flag.String("objstore.config-file", "", "")
	minTimeFlag := flag.String("min-time", "0", "")
	maxTimeFlag := flag.String("max-time", "0", "")
	delDelay := flag.Duration("delete-delay", 0, "")
	flag.Parse()

	err := minTime.Set(*minTimeFlag)
	if err != nil {
		panic(errors.Wrap(err, "could not parse min time"))
	}

	err = maxTime.Set(*maxTimeFlag)
	if err != nil {
		panic(errors.Wrap(err, "could not parse max time"))
	}

	bucketConf, err := ioutil.ReadFile(*bktFile)
	if err != nil {
		panic(errors.Wrap(err, "could not read bkt file"))
	}

	logger := log.NewLogfmtLogger(os.Stdout)

	bkt, err := client.NewBucket(
		logger,
		bucketConf,
		prometheus.DefaultRegisterer,
		"mb-thanos-blocks-deleter",
	)
	if err != nil {
		panic(errors.Wrap(err, "coud not init bucket"))
	}

	filter := thanosblock.NewIgnoreDeletionMarkFilter(logger, bkt, 0)

	f, err := thanosblock.NewMetaFetcher(
		logger,
		32,
		bkt,
		"",
		prometheus.DefaultRegisterer,
		[]thanosblock.MetadataFilter{filter},
		nil,
	)
	if err != nil {
		panic(errors.Wrap(err, "could not init fetcher"))
	}

	metas, _, err := f.Fetch(context.Background())
	if err != nil {
		panic(errors.Wrap(err, "could not fetch metas"))
	}

	markedForDeletion := promauto.NewCounter(prometheus.CounterOpts{
		Name:        "thanos_blocks_cleaner_marked_for_deletion",
		ConstLabels: nil,
	})

	for id, meta := range metas {
		if meta.MaxTime >= minTime.PrometheusTimestamp() && meta.MinTime <= maxTime.PrometheusTimestamp() {
			continue
		}
		err := thanosblock.MarkForDeletion(
			context.TODO(),
			logger,
			bkt,
			id,
			markedForDeletion,
		)
		if err != nil {
			panic(errors.Wrap(err, "could not mark block for deletion"))
		}
	}

	err = compact.NewBlocksCleaner(
		logger,
		bkt,
		filter,
		*delDelay,
		promauto.NewCounter(
			prometheus.CounterOpts{
				Name:        "thanos_blocks_cleaner_blocks_cleaned",
				ConstLabels: nil,
			},
		),
		promauto.NewCounter(
			prometheus.CounterOpts{
				Name:        "thanos_blocks_cleaner_cleanup_failures",
				ConstLabels: nil,
			},
		),
	).DeleteMarkedBlocks(context.TODO())
	if err != nil {
		panic(errors.Wrap(err, "could not delete marked blocks"))
	}
}
