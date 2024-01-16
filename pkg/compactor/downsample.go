package compactor

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/downsample"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/util/backoff"
	"github.com/cortexproject/cortex/pkg/util/concurrency"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/services"
)

const downsampleMetaPrefix = "downsample-meta-"

type DownsampleConfig struct {
	DownsampleInterval    time.Duration
	DownsampleConcurrency int
	blockSyncConcurrency  int
	metaSyncConcurrency   int
	consistencyDelay      time.Duration
	blockFilesConcurrency int
	acceptMalformedIndex  bool
	dataDir               string
	downsampleRetries     int

	// No need to add options to customize the retry backoff,
	// given the defaults should be fine, but allow to override
	// it in tests.
	retryMinBackoff time.Duration `yaml:"-"`
	retryMaxBackoff time.Duration `yaml:"-"`
}

type Downsample struct {
	services.Service

	reg          prometheus.Registerer
	cfg          DownsampleConfig
	cfgProvider  ConfigProvider
	logger       log.Logger
	bucketClient objstore.Bucket
	usersScanner *cortex_tsdb.UsersScanner
	metrics      *downsample.DownsampleMetrics

	// TSDB syncer metrics
	syncerMetrics *syncerMetrics
}

func NewDownsample(cfg DownsampleConfig, bucketClient objstore.Bucket, usersScanner *cortex_tsdb.UsersScanner, cfgProvider ConfigProvider, logger log.Logger, reg prometheus.Registerer, syncerMetrics *syncerMetrics) *Downsample {
	c := &Downsample{
		reg:           reg,
		cfg:           cfg,
		bucketClient:  bucketClient,
		usersScanner:  usersScanner,
		cfgProvider:   cfgProvider,
		logger:        log.With(logger, "component", "downsample"),
		syncerMetrics: syncerMetrics,
		metrics:       downsample.NewDownsampleMetrics(reg),
	}
    level.Info(c.logger).Log("msg", "creating NewTimerService")
	c.Service = services.NewTimerService(cfg.DownsampleInterval, c.starting, c.ticker, nil)
	level.Info(c.logger).Log("msg", "created NewTimerService")
	return c
}

func (c *Downsample) starting(ctx context.Context) error {
	// Run a cleanup so that any other service depending on this service
	// is guaranteed to start once the initial cleanup has been done.
	level.Info(c.logger).Log("msg", "calling rundownsample")
	c.runDownsample(ctx, true)
    
	return nil
}

func (c *Downsample) ticker(ctx context.Context) error {
	return c.downsampleUsers(ctx)
}

func (c *Downsample) runDownsample(ctx context.Context, firstRun bool) {
	level.Info(c.logger).Log("msg", "called downsample")

	if err := c.downsampleUsers(ctx); err == nil {
		level.Info(c.logger).Log("msg", "successfully completed blocks cleanup and maintenance")
	} else if errors.Is(err, context.Canceled) {
		level.Info(c.logger).Log("msg", "canceled blocks cleanup and maintenance", "err", err)
		return
	} else {
		level.Error(c.logger).Log("msg", "failed to run blocks cleanup and maintenance", "err", err.Error())
	}
}

func (c *Downsample) downsampleUsers(ctx context.Context) error {
	users, deleted, err := c.usersScanner.ScanUsers(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to discover users from bucket")
	}
	level.Info(c.logger).Log("msg", "after users scanned")
	allUsers := append(users, deleted...)
	ownedUsers := make(map[string]struct{}, len(allUsers))
	for _, u := range allUsers {
		ownedUsers[u] = struct{}{}
	}

	concurrency.ForEachUser(ctx, users, c.cfg.DownsampleConcurrency, func(ctx context.Context, userID string) error {
		level.Info(c.logger).Log("msg", "for each user")
		// Skipping downsample if the  bucket index failed to sync due to CMK errors.
		if idxs, err := bucketindex.ReadSyncStatus(ctx, c.bucketClient, userID, util_log.WithUserID(userID, c.logger)); err == nil {
			if idxs.Status == bucketindex.CustomerManagedKeyError {
				level.Info(c.logger).Log("msg", "skipping downsampleUser due CustomerManagedKeyError", "user", userID)
				return nil
			}
		}
		return errors.Wrapf(c.downsampleUserWithRetries(ctx, userID), "failed to delete blocks for user: %s", userID)
	})

	// Delete local files for unowned tenants, if there are any. This cleans up
	// leftover local files for tenants that belong to different compactors now,
	// or have been deleted completely.
	for userID := range c.listTenantsWithMetaSyncDirectories() {
		if _, owned := ownedUsers[userID]; owned {
			continue
		}

		dir := c.metaSyncDirForUser(userID)
		s, err := os.Stat(dir)
		if err != nil {
			if !os.IsNotExist(err) {
				level.Warn(c.logger).Log("msg", "failed to stat local directory with user data", "dir", dir, "err", err)
			}
			continue
		}

		if s.IsDir() {
			err := os.RemoveAll(dir)
			if err == nil {
				level.Info(c.logger).Log("msg", "deleted directory for user not owned by this shard", "dir", dir)
			} else {
				level.Warn(c.logger).Log("msg", "failed to delete directory for user not owned by this shard", "dir", dir, "err", err)
			}
		}
	}
	return nil
}

func (c *Downsample) downsampleUserWithRetries(ctx context.Context, userID string) error {
	var lastErr error
	level.Info(c.logger).Log("msg", "downsampleUserWithRetries")
	retries := backoff.New(ctx, backoff.Config{
		MinBackoff: c.cfg.retryMinBackoff,
		MaxBackoff: c.cfg.retryMaxBackoff,
		MaxRetries: c.cfg.downsampleRetries,
	})

	for retries.Ongoing() {
		lastErr = c.downsampleUser(ctx, userID)
		if lastErr == nil {
			return nil
		}

		retries.Wait()
	}

	return lastErr
}

func (c *Downsample) downsampleUser(ctx context.Context, userID string) (returnErr error) {
	bucket := bucket.NewUserBucketClient(userID, c.bucketClient, c.cfgProvider)
	reg := prometheus.NewRegistry()
	defer c.syncerMetrics.gatherThanosSyncerMetrics(reg)
	ulogger := util_log.WithUserID(userID, c.logger)

	// Filters out duplicate blocks that can be formed from two or more overlapping
	// blocks that fully submatches the source blocks of the older blocks.
	deduplicateBlocksFilter := block.NewDeduplicateFilter(c.cfg.blockSyncConcurrency)

	// While fetching blocks, we filter out blocks that were marked for deletion by using IgnoreDeletionMarkFilter.
	// No delay is used -- all blocks with deletion marker are ignored, and not considered for compaction.
	ignoreDeletionMarkFilter := block.NewIgnoreDeletionMarkFilter(
		ulogger,
		bucket,
		0,
		c.cfg.metaSyncConcurrency)

	// Filters out blocks with no compaction maker; blocks can be marked as no compaction for reasons like
	// out of order chunks or index file too big.
	noCompactMarkerFilter := compact.NewGatherNoCompactionMarkFilter(ulogger, bucket, c.cfg.metaSyncConcurrency)

	fetcher, err := block.NewMetaFetcher(
		ulogger,
		c.cfg.metaSyncConcurrency,
		bucket,
		c.metaSyncDirForUser(userID),
		reg,
		// List of filters to apply (order matters).
		[]block.MetadataFilter{
			// Remove the ingester ID because we don't shard blocks anymore, while still
			// honoring the shard ID if sharding was done in the past.
			NewLabelRemoverFilter([]string{cortex_tsdb.IngesterIDExternalLabel}),
			block.NewConsistencyDelayMetaFilter(ulogger, c.cfg.consistencyDelay, reg),
			ignoreDeletionMarkFilter,
			deduplicateBlocksFilter,
			noCompactMarkerFilter,
		},
	)
	if err != nil {
		return err
	}
	currentCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	level.Info(c.logger).Log("msg", "before fetcher")
	metas, _, err := fetcher.Fetch(currentCtx)
	level.Info(c.logger).Log("msg", "after fetcher")
	if err != nil {
		level.Info(c.logger).Log("msg", "after fetcher returning")
		return err
	}

	level.Info(c.logger).Log("msg", "before DownsampleBucket")

	if err := downsample.DownsampleBucket(
		currentCtx,
		ulogger,
		c.metrics,
		bucket,
		metas,
		path.Join(c.cfg.dataDir, "downsample"),
		c.cfg.DownsampleConcurrency,
		c.cfg.blockFilesConcurrency,
		metadata.NoneFunc,
		c.cfg.acceptMalformedIndex,
		downsample.ResLevel1DownsampleRange,
		downsample.ResLevel2DownsampleRange,
	); err != nil {
		level.Info(c.logger).Log("msg", "error received")
		return errors.Wrap(err, "downsample bucket")
	}
	return nil
}

func (c *Downsample) metaSyncDirForUser(userID string) string {
	return filepath.Join(c.cfg.dataDir, downsampleMetaPrefix+userID)
}

// This function returns tenants with meta sync directories found on local disk. On error, it returns nil map.
func (c *Downsample) listTenantsWithMetaSyncDirectories() map[string]struct{} {
	result := map[string]struct{}{}

	files, err := os.ReadDir(c.cfg.dataDir)
	if err != nil {
		return nil
	}

	for _, f := range files {
		if !f.IsDir() {
			continue
		}

		if !strings.HasPrefix(f.Name(), downsampleMetaPrefix) {
			continue
		}

		result[f.Name()[len(downsampleMetaPrefix):]] = struct{}{}
	}

	return result
}
