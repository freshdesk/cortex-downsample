package queryrange

import (
	"context"
	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/thanos-io/thanos/pkg/querysharding"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/tenant"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func EmbedQueryMiddleware(logger log.Logger, limits tripperware.Limits, queryAnalyzer querysharding.Analyzer, engine v1.QueryEngine) tripperware.Middleware {
	return tripperware.MiddlewareFunc(func(next tripperware.Handler) tripperware.Handler {
		return embedQuery{
			next:     next,
			limits:   limits,
			logger:   logger,
			analyzer: queryAnalyzer,
			engine:   engine,
		}
	})
}

type embedQuery struct {
	next     tripperware.Handler
	limits   tripperware.Limits
	logger   log.Logger
	analyzer querysharding.Analyzer

	engine v1.QueryEngine
}

func (s embedQuery) Do(ctx context.Context, r tripperware.Request) (tripperware.Response, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, err.Error())
	}

	numShards := validation.SmallestPositiveIntPerTenant(tenantIDs, s.limits.QueryVerticalShardSize)
	if numShards <= 1 {
		return s.next.Do(ctx, r)
	}

	logger := util_log.WithContext(ctx, s.logger)
	analysis, err := s.analyzer.Analyze(r.GetQuery())
	if err != nil {
		level.Warn(logger).Log("msg", "error analyzing query", "q", r.GetQuery(), "err", err)
	}
	if analysis.IsShardable() {
		return s.next.Do(ctx, r)
	}

	expr, err := parser.ParseExpr(r.GetQuery())
	if err != nil {
		return s.next.Do(ctx, r)
	}
	switch n := expr.(type) {
	case *parser.AggregateExpr:
		if len(n.Grouping) == 0 && n.Op == parser.SUM {
			// Parse inner expr.
			// Ignore error for now.
			innerQuery := n.Expr.String()
			analysis, err := s.analyzer.Analyze(innerQuery)
			if err != nil {
				return s.next.Do(ctx, r)
			}
			// We can try to push down.
			if analysis.IsShardable() {
				n.Expr = &parser.VectorSelector{LabelMatchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, tripperware.QueryLabel, innerQuery)}}
				return s.evaluateWithQueryEngine(ctx, r.WithQuery(n.String()))
			}
		}
	}

	return s.next.Do(ctx, r)
}

func (s embedQuery) evaluateWithQueryEngine(ctx context.Context, r tripperware.Request) (tripperware.Response, error) {
	queryable := &tripperware.RemoteQueryable{Req: r, Next: s.next, RespToSeriesSetFunc: convert}
	qry, err := s.engine.NewRangeQuery(
		ctx,
		queryable,
		nil,
		r.GetQuery(),
		util.TimeFromMillis(r.GetStart()),
		util.TimeFromMillis(r.GetEnd()),
		time.Duration(r.GetStep())*time.Millisecond,
	)
	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, err.Error())
	}
	result := qry.Exec(ctx)
	sampleStreams, err := FromResult(result)
	if err != nil {
		return nil, err
	}

	return &PrometheusResponse{
		Data: PrometheusData{
			ResultType: model.ValMatrix.String(),
			Result:     sampleStreams,
		},
	}, nil
}

func convert(sortSeries bool, resp tripperware.Response) storage.SeriesSet {
	streams, err := ResponseToSamples(resp)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	return NewSeriesSet(sortSeries, streams)
}
