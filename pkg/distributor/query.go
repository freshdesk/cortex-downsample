package distributor

import (
	"context"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/weaveworks/common/instrument"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	ingester_client "github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier/stats"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/extract"
	"github.com/cortexproject/cortex/pkg/util/grpcutil"
	"github.com/cortexproject/cortex/pkg/util/limiter"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

// Query multiple ingesters and returns a Matrix of samples.
func (d *Distributor) Query(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (model.Matrix, error) {
	var matrix model.Matrix
	err := instrument.CollectedRequest(ctx, "Distributor.Query", d.queryDuration, instrument.ErrorCode, func(ctx context.Context) error {
		req, err := ingester_client.ToQueryRequest(from, to, matchers)
		if err != nil {
			return err
		}

		replicationSet, err := d.GetIngestersForQuery(ctx, matchers...)
		if err != nil {
			return err
		}

		matrix, err = d.queryIngesters(ctx, replicationSet, req)
		if err != nil {
			return err
		}

		if s := opentracing.SpanFromContext(ctx); s != nil {
			s.LogKV("series", len(matrix))
		}
		return nil
	})
	return matrix, err
}

func (d *Distributor) QueryExemplars(ctx context.Context, from, to model.Time, matchers ...[]*labels.Matcher) (*ingester_client.ExemplarQueryResponse, error) {
	var result *ingester_client.ExemplarQueryResponse
	err := instrument.CollectedRequest(ctx, "Distributor.QueryExemplars", d.queryDuration, instrument.ErrorCode, func(ctx context.Context) error {
		req, err := ingester_client.ToExemplarQueryRequest(from, to, matchers...)
		if err != nil {
			return err
		}

		// We ask for all ingesters without passing matchers because exemplar queries take in an array of array of label matchers.
		replicationSet, err := d.GetIngestersForQuery(ctx)
		if err != nil {
			return err
		}

		result, err = d.queryIngestersExemplars(ctx, replicationSet, req)
		if err != nil {
			return err
		}

		if s := opentracing.SpanFromContext(ctx); s != nil {
			s.LogKV("series", len(result.Timeseries))
		}
		return nil
	})
	return result, err
}

// QueryStream multiple ingesters via the streaming interface and returns big ol' set of chunks.
func (d *Distributor) QueryStream(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (*ingester_client.QueryStreamResponse, error) {
	var result *ingester_client.QueryStreamResponse
	err := instrument.CollectedRequest(ctx, "Distributor.QueryStream", d.queryDuration, instrument.ErrorCode, func(ctx context.Context) error {
		req, err := ingester_client.ToQueryRequest(from, to, matchers)
		if err != nil {
			return err
		}

		replicationSet, err := d.GetIngestersForQuery(ctx, matchers...)
		if err != nil {
			return err
		}

		result, err = d.queryIngesterStream(ctx, replicationSet, req)
		if err != nil {
			return err
		}

		if s := opentracing.SpanFromContext(ctx); s != nil {
			s.LogKV("chunk-series", len(result.GetChunkseries()), "time-series", len(result.GetTimeseries()))
		}
		return nil
	})
	return result, err
}

// GetIngestersForQuery returns a replication set including all ingesters that should be queried
// to fetch series matching input label matchers.
func (d *Distributor) GetIngestersForQuery(ctx context.Context, matchers ...*labels.Matcher) (ring.ReplicationSet, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return ring.ReplicationSet{}, err
	}

	// If shuffle sharding is enabled we should only query ingesters which are
	// part of the tenant's subring.
	if d.cfg.ShardingStrategy == util.ShardingStrategyShuffle {
		shardSize := d.limits.IngestionTenantShardSize(userID)
		lookbackPeriod := d.cfg.ShuffleShardingLookbackPeriod

		if shardSize > 0 && lookbackPeriod > 0 {
			return d.ingestersRing.ShuffleShardWithLookback(userID, shardSize, lookbackPeriod, time.Now()).GetReplicationSetForOperation(ring.Read)
		}
	}

	// If "shard by all labels" is disabled, we can get ingesters by metricName if exists.
	if !d.cfg.ShardByAllLabels && len(matchers) > 0 {
		metricNameMatcher, _, ok := extract.MetricNameMatcherFromMatchers(matchers)

		if ok && metricNameMatcher.Type == labels.MatchEqual {
			return d.ingestersRing.Get(shardByMetricName(userID, metricNameMatcher.Value), ring.Read, nil, nil, nil)
		}
	}

	return d.ingestersRing.GetReplicationSetForOperation(ring.Read)
}

// GetIngestersForMetadata returns a replication set including all ingesters that should be queried
// to fetch metadata (eg. label names/values or series).
func (d *Distributor) GetIngestersForMetadata(ctx context.Context) (ring.ReplicationSet, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return ring.ReplicationSet{}, err
	}

	// If shuffle sharding is enabled we should only query ingesters which are
	// part of the tenant's subring.
	if d.cfg.ShardingStrategy == util.ShardingStrategyShuffle {
		shardSize := d.limits.IngestionTenantShardSize(userID)
		lookbackPeriod := d.cfg.ShuffleShardingLookbackPeriod

		if shardSize > 0 && lookbackPeriod > 0 {
			return d.ingestersRing.ShuffleShardWithLookback(userID, shardSize, lookbackPeriod, time.Now()).GetReplicationSetForOperation(ring.Read)
		}
	}

	return d.ingestersRing.GetReplicationSetForOperation(ring.Read)
}

// queryIngesters queries the ingesters via the older, sample-based API.
func (d *Distributor) queryIngesters(ctx context.Context, replicationSet ring.ReplicationSet, req *ingester_client.QueryRequest) (model.Matrix, error) {
	// Fetch samples from multiple ingesters in parallel, using the replicationSet
	// to deal with consistency.
	results, err := replicationSet.Do(ctx, d.cfg.ExtraQueryDelay, func(ctx context.Context, ing *ring.InstanceDesc) (interface{}, error) {
		client, err := d.ingesterPool.GetClientFor(ing.Addr)
		if err != nil {
			return nil, err
		}

		resp, err := client.(ingester_client.IngesterClient).Query(ctx, req)
		d.ingesterQueries.WithLabelValues(ing.Addr).Inc()
		if err != nil {
			d.ingesterQueryFailures.WithLabelValues(ing.Addr).Inc()
			return nil, err
		}

		return ingester_client.FromQueryResponse(resp), nil
	})
	if err != nil {
		return nil, err
	}

	// Merge the results into a single matrix.
	fpToSampleStream := map[model.Fingerprint]*model.SampleStream{}
	for _, result := range results {
		for _, ss := range result.(model.Matrix) {
			fp := ss.Metric.Fingerprint()
			mss, ok := fpToSampleStream[fp]
			if !ok {
				mss = &model.SampleStream{
					Metric: ss.Metric,
				}
				fpToSampleStream[fp] = mss
			}
			mss.Values = util.MergeSampleSets(mss.Values, ss.Values)
		}
	}
	result := model.Matrix{}
	for _, ss := range fpToSampleStream {
		result = append(result, ss)
	}

	return result, nil
}

// mergeExemplarSets merges and dedupes two sets of already sorted exemplar pairs.
// Both a and b should be lists of exemplars from the same series.
// Defined here instead of pkg/util to avoid a import cycle.
func mergeExemplarSets(a, b []cortexpb.Exemplar) []cortexpb.Exemplar {
	result := make([]cortexpb.Exemplar, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i].TimestampMs < b[j].TimestampMs {
			result = append(result, a[i])
			i++
		} else if a[i].TimestampMs > b[j].TimestampMs {
			result = append(result, b[j])
			j++
		} else {
			result = append(result, a[i])
			i++
			j++
		}
	}
	// Add the rest of a or b. One of them is empty now.
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)
	return result
}

// queryIngestersExemplars queries the ingesters for exemplars.
func (d *Distributor) queryIngestersExemplars(ctx context.Context, replicationSet ring.ReplicationSet, req *ingester_client.ExemplarQueryRequest) (*ingester_client.ExemplarQueryResponse, error) {
	// Fetch exemplars from multiple ingesters in parallel, using the replicationSet
	// to deal with consistency.
	results, err := replicationSet.Do(ctx, d.cfg.ExtraQueryDelay, func(ctx context.Context, ing *ring.InstanceDesc) (interface{}, error) {
		client, err := d.ingesterPool.GetClientFor(ing.Addr)
		if err != nil {
			return nil, err
		}

		resp, err := client.(ingester_client.IngesterClient).QueryExemplars(ctx, req)
		d.ingesterQueries.WithLabelValues(ing.Addr).Inc()
		if err != nil {
			d.ingesterQueryFailures.WithLabelValues(ing.Addr).Inc()
			return nil, err
		}

		return resp, nil
	})
	if err != nil {
		return nil, err
	}

	return mergeExemplarQueryResponses(results), nil
}

func mergeExemplarQueryResponses(results []interface{}) *ingester_client.ExemplarQueryResponse {
	var keys []string
	exemplarResults := make(map[string]cortexpb.TimeSeries)
	buf := make([]byte, 0, 1024)
	for _, result := range results {
		r := result.(*ingester_client.ExemplarQueryResponse)
		for _, ts := range r.Timeseries {
			lbls := string(cortexpb.FromLabelAdaptersToLabels(ts.Labels).Bytes(buf))
			e, ok := exemplarResults[lbls]
			if !ok {
				exemplarResults[lbls] = ts
				keys = append(keys, lbls)
			} else {
				// Merge in any missing values from another ingesters exemplars for this series.
				e.Exemplars = mergeExemplarSets(e.Exemplars, ts.Exemplars)
				exemplarResults[lbls] = e
			}
		}
	}

	// Query results from each ingester were sorted, but are not necessarily still sorted after merging.
	sort.Strings(keys)

	result := make([]cortexpb.TimeSeries, len(exemplarResults))
	for i, k := range keys {
		result[i] = exemplarResults[k]
	}

	return &ingester_client.ExemplarQueryResponse{Timeseries: result}
}

// queryIngesterStream queries the ingesters using the new streaming API.
func (d *Distributor) queryIngesterStream(ctx context.Context, replicationSet ring.ReplicationSet, req *ingester_client.QueryRequest) (*ingester_client.QueryStreamResponse, error) {
	var (
		queryLimiter = limiter.QueryLimiterFromContextWithFallback(ctx)
		reqStats     = stats.FromContext(ctx)
	)

	chCS := make(chan []ingester_client.TimeSeriesChunk, len(replicationSet.Instances))
	chTS := make(chan []cortexpb.TimeSeries, len(replicationSet.Instances))
	doneCh := make(chan struct{})

	hashToChunkseries := make(map[string]ingester_client.TimeSeriesChunk, 128)
	hashToTimeSeries := map[string]cortexpb.TimeSeries{}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case cs := <-chCS:
				for _, series := range cs {
					key := ingester_client.LabelsToKeyString(cortexpb.FromLabelAdaptersToLabels(series.Labels))
					existing := hashToChunkseries[key]
					existing.Labels = series.Labels
					existing.Chunks = append(existing.Chunks, series.Chunks...)
					hashToChunkseries[key] = existing
				}
			case ts := <-chTS:
				for _, series := range ts {
					key := ingester_client.LabelsToKeyString(cortexpb.FromLabelAdaptersToLabels(series.Labels))
					existing := hashToTimeSeries[key]
					existing.Labels = series.Labels
					if existing.Samples == nil {
						existing.Samples = series.Samples
					} else {
						existing.Samples = mergeSamples(existing.Samples, series.Samples)
					}
					hashToTimeSeries[key] = existing
				}
			case <-doneCh:
				return
			}
		}
	}()

	// Fetch samples from multiple ingesters
	_, err := replicationSet.Do(ctx, d.cfg.ExtraQueryDelay, func(ctx context.Context, ing *ring.InstanceDesc) (interface{}, error) {
		client, err := d.ingesterPool.GetClientFor(ing.Addr)
		if err != nil {
			return nil, err
		}
		d.ingesterQueries.WithLabelValues(ing.Addr).Inc()

		stream, err := client.(ingester_client.IngesterClient).QueryStream(ctx, req)
		if err != nil {
			d.ingesterQueryFailures.WithLabelValues(ing.Addr).Inc()
			return nil, err
		}
		defer stream.CloseSend() //nolint:errcheck

		result := &ingester_client.QueryStreamResponse{}
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			} else if err != nil {
				// Do not track a failure if the context was canceled.
				if !grpcutil.IsGRPCContextCanceled(err) {
					d.ingesterQueryFailures.WithLabelValues(ing.Addr).Inc()
				}

				return nil, err
			}

			// Enforce the max chunks limits.
			if chunkLimitErr := queryLimiter.AddChunks(resp.ChunksCount()); chunkLimitErr != nil {
				return nil, validation.LimitError(chunkLimitErr.Error())
			}

			for _, series := range resp.Chunkseries {
				if limitErr := queryLimiter.AddSeries(series.Labels); limitErr != nil {
					return nil, validation.LimitError(limitErr.Error())
				}
			}

			if chunkBytesLimitErr := queryLimiter.AddChunkBytes(resp.ChunksSize()); chunkBytesLimitErr != nil {
				return nil, validation.LimitError(chunkBytesLimitErr.Error())
			}

			if dataBytesLimitErr := queryLimiter.AddDataBytes(resp.Size()); dataBytesLimitErr != nil {
				return nil, validation.LimitError(dataBytesLimitErr.Error())
			}

			for _, series := range resp.Timeseries {
				if limitErr := queryLimiter.AddSeries(series.Labels); limitErr != nil {
					return nil, validation.LimitError(limitErr.Error())
				}
			}

			if len(resp.Chunkseries) > 0 {
				chCS <- resp.Chunkseries
			}
			if len(resp.Timeseries) > 0 {
				chTS <- resp.Timeseries
			}
		}
		return result, nil
	})
	doneCh <- struct{}{}
	wg.Wait()
	close(doneCh)
	if err != nil {
		return nil, err
	}

	resp := &ingester_client.QueryStreamResponse{
		Chunkseries: make([]ingester_client.TimeSeriesChunk, 0, len(hashToChunkseries)),
		Timeseries:  make([]cortexpb.TimeSeries, 0, len(hashToTimeSeries)),
	}
	for _, series := range hashToChunkseries {
		resp.Chunkseries = append(resp.Chunkseries, series)
	}
	for _, series := range hashToTimeSeries {
		resp.Timeseries = append(resp.Timeseries, series)
	}

	reqStats.AddFetchedSeries(uint64(len(resp.Chunkseries) + len(resp.Timeseries)))
	reqStats.AddFetchedChunkBytes(uint64(resp.ChunksSize()))
	reqStats.AddFetchedDataBytes(uint64(resp.Size()))
	reqStats.AddFetchedChunks(uint64(resp.ChunksCount()))
	reqStats.AddFetchedSamples(uint64(resp.SamplesCount()))

	return resp, nil
}

// Merges and dedupes two sorted slices with samples together.
func mergeSamples(a, b []cortexpb.Sample) []cortexpb.Sample {
	if sameSamples(a, b) {
		return a
	}

	result := make([]cortexpb.Sample, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i].TimestampMs < b[j].TimestampMs {
			result = append(result, a[i])
			i++
		} else if a[i].TimestampMs > b[j].TimestampMs {
			result = append(result, b[j])
			j++
		} else {
			result = append(result, a[i])
			i++
			j++
		}
	}
	// Add the rest of a or b. One of them is empty now.
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)
	return result
}

func sameSamples(a, b []cortexpb.Sample) bool {
	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
