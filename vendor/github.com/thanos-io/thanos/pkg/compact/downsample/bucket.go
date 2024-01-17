package downsample

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/runutil"
)

type DownsampleMetrics struct {
	Downsamples        *prometheus.CounterVec
	DownsampleFailures *prometheus.CounterVec
	DownsampleDuration *prometheus.HistogramVec
}

func NewDownsampleMetrics(reg prometheus.Registerer) *DownsampleMetrics {
	m := new(DownsampleMetrics)

	m.Downsamples = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_compact_downsample_total",
		Help: "Total number of downsampling attempts.",
	}, []string{"group"})
	m.DownsampleFailures = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_compact_downsample_failures_total",
		Help: "Total number of failed downsampling attempts.",
	}, []string{"group"})
	m.DownsampleDuration = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "thanos_compact_downsample_duration_seconds",
		Help:    "Duration of downsample runs",
		Buckets: []float64{60, 300, 900, 1800, 3600, 7200, 14400}, // 1m, 5m, 15m, 30m, 60m, 120m, 240m
	}, []string{"group"})

	return m
}

func DownsampleBucket(
	ctx context.Context,
	logger log.Logger,
	metrics *DownsampleMetrics,
	bkt objstore.Bucket,
	metas map[ulid.ULID]*metadata.Meta,
	dir string,
	downsampleConcurrency int,
	blockFilesConcurrency int,
	hashFunc metadata.HashFunc,
	acceptMalformedIndex bool,
	level1DownsampleRange, level2DownsampleRange int64,
) (rerr error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return errors.Wrap(err, "create dir")
	}

	defer func() {
		// Leave the downsample directory for inspection if it is a halt error
		// or if it is not then so that possibly we would not have to download everything again.
		if rerr != nil {
			return
		}
		if err := os.RemoveAll(dir); err != nil {
			level.Error(logger).Log("msg", "failed to remove downsample cache directory", "path", dir, "err", err)
		}
	}()

	// mapping from a hash over all source IDs to blocks. We don't need to downsample a block
	// if a downsampled version with the same hash already exists.
	sources5m := map[ulid.ULID]struct{}{}
	sources1h := map[ulid.ULID]struct{}{}

	for _, m := range metas {
		switch m.Thanos.Downsample.Resolution {
		case ResLevel0:
			continue
		case ResLevel1:
			for _, id := range m.Compaction.Sources {
				sources5m[id] = struct{}{}
			}
		case ResLevel2:
			for _, id := range m.Compaction.Sources {
				sources1h[id] = struct{}{}
			}
		default:
			return errors.Errorf("unexpected downsampling resolution %d", m.Thanos.Downsample.Resolution)
		}
	}

	ignoreDirs := []string{}
	for ulid := range metas {
		ignoreDirs = append(ignoreDirs, ulid.String())
	}

	if err := runutil.DeleteAll(dir, ignoreDirs...); err != nil {
		level.Warn(logger).Log("msg", "failed deleting potentially outdated directories/files, some disk space usage might have leaked. Continuing", "err", err, "dir", dir)
	}

	metasULIDS := make([]ulid.ULID, 0, len(metas))
	for k := range metas {
		metasULIDS = append(metasULIDS, k)
	}
	sort.Slice(metasULIDS, func(i, j int) bool {
		return metasULIDS[i].Compare(metasULIDS[j]) < 0
	})

	var (
		wg                      sync.WaitGroup
		metaCh                  = make(chan *metadata.Meta)
		downsampleErrs          errutil.MultiError
		errCh                   = make(chan error, downsampleConcurrency)
		workerCtx, workerCancel = context.WithCancel(ctx)
	)

	defer workerCancel()

	level.Debug(logger).Log("msg", "downsampling bucket", "concurrency", downsampleConcurrency)
	for i := 0; i < downsampleConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for m := range metaCh {
				resolution := ResLevel1
				errMsg := "downsampling to 5 min"
				if m.Thanos.Downsample.Resolution == ResLevel1 {
					resolution = ResLevel2
					errMsg = "downsampling to 60 min"
				}
				if err := processDownsampling(workerCtx, logger, bkt, m, dir, resolution, hashFunc, metrics, acceptMalformedIndex, blockFilesConcurrency); err != nil {
					metrics.DownsampleFailures.WithLabelValues(m.Thanos.GroupKey()).Inc()
					errCh <- errors.Wrap(err, errMsg)

				}
				metrics.Downsamples.WithLabelValues(m.Thanos.GroupKey()).Inc()
			}
		}()
	}

	// Workers scheduled, distribute blocks.
metaSendLoop:
	for _, mk := range metasULIDS {
		m := metas[mk]

		switch m.Thanos.Downsample.Resolution {
		case ResLevel2:
			continue

		case ResLevel0:
			missing := false
			for _, id := range m.Compaction.Sources {
				if _, ok := sources5m[id]; !ok {
					missing = true
					break
				}
			}
			if !missing {
				continue
			}
			
			// downsample only for 30 days old blocks

			if time.Now().Sub(time.Unix(m.MaxTime/1000, 0))  <  (30 * 24 * time.Hour) {
				continue
			}

			// Only downsample blocks once we are sure to get roughly 2 chunks out of it.
			// NOTE(fabxc): this must match with at which block size the compactor creates downsampled
			// blocks. Otherwise we may never downsample some data.
			if m.MaxTime-m.MinTime < level1DownsampleRange {
				continue
			}

		case ResLevel1:
			missing := false
			for _, id := range m.Compaction.Sources {
				if _, ok := sources1h[id]; !ok {
					missing = true
					break
				}
			}
			if !missing {
				continue
			}
			// Only downsample blocks once we are sure to get roughly 2 chunks out of it.
			// NOTE(fabxc): this must match with at which block size the compactor creates downsampled
			// blocks. Otherwise we may never downsample some data.
			if m.MaxTime-m.MinTime < level2DownsampleRange {
				continue
			}
		}

		select {
		case <-workerCtx.Done():
			downsampleErrs.Add(workerCtx.Err())
			break metaSendLoop
		case metaCh <- m:
		case downsampleErr := <-errCh:
			downsampleErrs.Add(downsampleErr)
			break metaSendLoop
		}
	}

	close(metaCh)
	wg.Wait()
	workerCancel()
	close(errCh)

	// Collect any other error reported by the workers.
	for downsampleErr := range errCh {
		downsampleErrs.Add(downsampleErr)
	}

	return downsampleErrs.Err()
}

func processDownsampling(
	ctx context.Context,
	logger log.Logger,
	bkt objstore.Bucket,
	m *metadata.Meta,
	dir string,
	resolution int64,
	hashFunc metadata.HashFunc,
	metrics *DownsampleMetrics,
	acceptMalformedIndex bool,
	blockFilesConcurrency int,
) error {
	begin := time.Now()
	bdir := filepath.Join(dir, m.ULID.String())

	err := block.Download(ctx, logger, bkt, m.ULID, bdir, objstore.WithFetchConcurrency(blockFilesConcurrency))
	if err != nil {
		return errors.Wrapf(err, "download block %s", m.ULID)
	}
	level.Info(logger).Log("msg", "downloaded block", "id", m.ULID, "duration", time.Since(begin), "duration_ms", time.Since(begin).Milliseconds())

	if err := block.VerifyIndex(logger, filepath.Join(bdir, block.IndexFilename), m.MinTime, m.MaxTime); err != nil && !acceptMalformedIndex {
		return errors.Wrap(err, "input block index not valid")
	}

	begin = time.Now()

	var pool chunkenc.Pool
	if m.Thanos.Downsample.Resolution == 0 {
		pool = chunkenc.NewPool()
	} else {
		pool = NewPool()
	}

	b, err := tsdb.OpenBlock(logger, bdir, pool)
	if err != nil {
		return errors.Wrapf(err, "open block %s", m.ULID)
	}
	defer runutil.CloseWithLogOnErr(log.With(logger, "outcome", "potential left mmap file handlers left"), b, "tsdb reader")

	id, err := downsample(logger, m, b, dir, resolution)
	if err != nil {
		return errors.Wrapf(err, "downsample block %s to window %d", m.ULID, resolution)
	}
	resdir := filepath.Join(dir, id.String())

	downsampleDuration := time.Since(begin)
	level.Info(logger).Log("msg", "downsampled block",
		"from", m.ULID, "to", id, "duration", downsampleDuration, "duration_ms", downsampleDuration.Milliseconds())
	metrics.DownsampleDuration.WithLabelValues(m.Thanos.GroupKey()).Observe(downsampleDuration.Seconds())

	stats, err := block.GatherIndexHealthStats(logger, filepath.Join(resdir, block.IndexFilename), m.MinTime, m.MaxTime)
	if err == nil {
		err = stats.AnyErr()
	}
	if err != nil && !acceptMalformedIndex {
		return errors.Wrap(err, "output block index not valid")
	}

	meta, err := metadata.ReadFromDir(resdir)
	if err != nil {
		return errors.Wrap(err, "read meta")
	}

	if stats.ChunkMaxSize > 0 {
		meta.Thanos.IndexStats.ChunkMaxSize = stats.ChunkMaxSize
	}
	if stats.SeriesMaxSize > 0 {
		meta.Thanos.IndexStats.SeriesMaxSize = stats.SeriesMaxSize
	}
	if err := meta.WriteToDir(logger, resdir); err != nil {
		return errors.Wrap(err, "write meta")
	}

	begin = time.Now()

	err = block.Upload(ctx, logger, bkt, resdir, hashFunc)
	if err != nil {
		return errors.Wrapf(err, "upload downsampled block %s", id)
	}

	level.Info(logger).Log("msg", "uploaded block", "id", id, "duration", time.Since(begin), "duration_ms", time.Since(begin).Milliseconds())

	// It is not harmful if these fails.
	if err := os.RemoveAll(bdir); err != nil {
		level.Warn(logger).Log("msg", "failed to clean directory", "dir", bdir, "err", err)
	}
	if err := os.RemoveAll(resdir); err != nil {
		level.Warn(logger).Log("msg", "failed to clean directory", "resdir", bdir, "err", err)
	}

	return nil
}
