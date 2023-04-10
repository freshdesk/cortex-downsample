package api

import (
	"context"
	"encoding/json"
	"github.com/thanos-io/thanos/pkg/api"
	"html/template"
	"net/http"
	"path"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gorilla/mux"
	"github.com/grafana/regexp"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/route"
	"github.com/prometheus/common/version"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/storage"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/weaveworks/common/instrument"
	"github.com/weaveworks/common/middleware"

	"github.com/cortexproject/cortex/pkg/purger"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/querier/stats"
	"github.com/cortexproject/cortex/pkg/util"
)

const (
	SectionAdminEndpoints = "Admin Endpoints:"
	SectionDangerous      = "Dangerous:"
)

func newIndexPageContent() *IndexPageContent {
	return &IndexPageContent{
		content: map[string]map[string]string{},
	}
}

// IndexPageContent is a map of sections to path -> description.
type IndexPageContent struct {
	mu      sync.Mutex
	content map[string]map[string]string
}

func (pc *IndexPageContent) AddLink(section, path, description string) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	sectionMap := pc.content[section]
	if sectionMap == nil {
		sectionMap = make(map[string]string)
		pc.content[section] = sectionMap
	}

	sectionMap[path] = description
}

func (pc *IndexPageContent) GetContent() map[string]map[string]string {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	result := map[string]map[string]string{}
	for k, v := range pc.content {
		sm := map[string]string{}
		for smK, smV := range v {
			sm[smK] = smV
		}
		result[k] = sm
	}
	return result
}

var indexPageTemplate = `
<!DOCTYPE html>
<html>
	<head>
		<meta charset="UTF-8">
		<title>Cortex</title>
	</head>
	<body>
		<h1>Cortex</h1>
		{{ range $s, $links := . }}
		<p>{{ $s }}</p>
		<ul>
			{{ range $path, $desc := $links }}
				<li><a href="{{ AddPathPrefix $path }}">{{ $desc }}</a></li>
			{{ end }}
		</ul>
		{{ end }}
	</body>
</html>`

func indexHandler(httpPathPrefix string, content *IndexPageContent) http.HandlerFunc {
	templ := template.New("main")
	templ.Funcs(map[string]interface{}{
		"AddPathPrefix": func(link string) string {
			return path.Join(httpPathPrefix, link)
		},
	})
	template.Must(templ.Parse(indexPageTemplate))

	return func(w http.ResponseWriter, r *http.Request) {
		err := templ.Execute(w, content.GetContent())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func (cfg *Config) configHandler(actualCfg interface{}, defaultCfg interface{}) http.HandlerFunc {
	if cfg.CustomConfigHandler != nil {
		return cfg.CustomConfigHandler(actualCfg, defaultCfg)
	}
	return DefaultConfigHandler(actualCfg, defaultCfg)
}

func DefaultConfigHandler(actualCfg interface{}, defaultCfg interface{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var output interface{}
		switch r.URL.Query().Get("mode") {
		case "diff":
			defaultCfgObj, err := util.YAMLMarshalUnmarshal(defaultCfg)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			actualCfgObj, err := util.YAMLMarshalUnmarshal(actualCfg)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			diff, err := util.DiffConfig(defaultCfgObj, actualCfgObj)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			output = diff

		case "defaults":
			output = defaultCfg
		default:
			output = actualCfg
		}

		util.WriteYAMLResponse(w, output)
	}
}

// NewQuerierHandler returns a HTTP handler that can be used by the querier service to
// either register with the frontend worker query processor or with the external HTTP
// server to fulfill the Prometheus query API.
func NewQuerierHandler(
	cfg Config,
	queryable storage.SampleAndChunkQueryable,
	exemplarQueryable storage.ExemplarQueryable,
	engine v1.QueryEngine,
	distributor Distributor,
	tombstonesLoader purger.TombstonesLoader,
	reg prometheus.Registerer,
	logger log.Logger,
	queryStoreAfter time.Duration,
) http.Handler {
	// Prometheus histograms for requests to the querier.
	querierRequestDuration := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "querier_request_duration_seconds",
		Help:      "Time (in seconds) spent serving HTTP requests to the querier.",
		Buckets:   instrument.DefBuckets,
	}, []string{"method", "route", "status_code", "ws"})

	receivedMessageSize := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "querier_request_message_bytes",
		Help:      "Size (in bytes) of messages received in the request to the querier.",
		Buckets:   middleware.BodySizeBuckets,
	}, []string{"method", "route"})

	sentMessageSize := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "querier_response_message_bytes",
		Help:      "Size (in bytes) of messages sent in response by the querier.",
		Buckets:   middleware.BodySizeBuckets,
	}, []string{"method", "route"})

	inflightRequests := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "querier_inflight_requests",
		Help:      "Current number of inflight requests to the querier.",
	}, []string{"method", "route"})

	v1api := v1.NewAPI(
		engine,
		querier.NewErrorTranslateSampleAndChunkQueryable(queryable), // Translate errors to errors expected by API.
		nil, // No remote write support.
		exemplarQueryable,
		func(ctx context.Context) v1.ScrapePoolsRetriever { return nil },
		func(context.Context) v1.TargetRetriever { return &querier.DummyTargetRetriever{} },
		func(context.Context) v1.AlertmanagerRetriever { return &querier.DummyAlertmanagerRetriever{} },
		func() config.Config { return config.Config{} },
		map[string]string{}, // TODO: include configuration flags
		v1.GlobalURLOptions{},
		func(f http.HandlerFunc) http.HandlerFunc { return f },
		nil,   // Only needed for admin APIs.
		"",    // This is for snapshots, which is disabled when admin APIs are disabled. Hence empty.
		false, // Disable admin APIs.
		logger,
		func(context.Context) v1.RulesRetriever { return &querier.DummyRulesRetriever{} },
		0, 0, 0, // Remote read samples and concurrency limit.
		false,
		regexp.MustCompile(".*"),
		func() (v1.RuntimeInfo, error) { return v1.RuntimeInfo{}, errors.New("not implemented") },
		&v1.PrometheusVersion{
			Version:   version.Version,
			Branch:    version.Branch,
			Revision:  version.Revision,
			BuildUser: version.BuildUser,
			BuildDate: version.BuildDate,
			GoVersion: version.GoVersion,
		},
		// This is used for the stats API which we should not support. Or find other ways to.
		prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) { return nil, nil }),
		reg,
		nil,
	)

	router := mux.NewRouter()

	// Use a separate metric for the querier in order to differentiate requests from the query-frontend when
	// running Cortex as a single binary.
	inst := middleware.Instrument{
		RouteMatcher:     router,
		Duration:         querierRequestDuration,
		RequestBodySize:  receivedMessageSize,
		ResponseBodySize: sentMessageSize,
		InflightRequests: inflightRequests,
	}
	cacheGenHeaderMiddleware := getHTTPCacheGenNumberHeaderSetterMiddleware(tombstonesLoader)
	middlewares := middleware.Merge(inst, cacheGenHeaderMiddleware)
	router.Use(middlewares.Wrap)

	// Define the prefixes for all routes
	prefix := path.Join(cfg.ServerPrefix, cfg.PrometheusHTTPPrefix)
	legacyPrefix := path.Join(cfg.ServerPrefix, cfg.LegacyHTTPPrefix)

	promRouter := route.New().WithPrefix(path.Join(prefix, "/api/v1"))
	v1api.Register(promRouter)

	legacyPromRouter := route.New().WithPrefix(path.Join(legacyPrefix, "/api/v1"))
	v1api.Register(legacyPromRouter)

	wrap := func(f api.ApiFunc) http.HandlerFunc {
		hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if data, warnings, err, releaseResources := f(r); err != nil {
				RespondError(w, err, data)
				releaseResources()
			} else if data != nil {
				Respond(w, data, warnings)
				releaseResources()
			} else {
				w.WriteHeader(http.StatusNoContent)
				releaseResources()
			}
		})
		return hf
	}
	qapi := querier.API{
		Queryable:       queryable,
		QueryEngine:     engine,
		QueryStoreAfter: queryStoreAfter,
	}

	// TODO(gotjosh): This custom handler is temporary until we're able to vendor the changes in:
	// https://github.com/prometheus/prometheus/pull/7125/files
	router.Path(path.Join(prefix, "/api/v1/metadata")).Handler(querier.MetadataHandler(distributor))
	router.Path(path.Join(prefix, "/api/v1/read")).Handler(querier.RemoteReadHandler(queryable, logger))
	router.Path(path.Join(prefix, "/api/v1/read")).Methods("POST").Handler(promRouter)
	router.Path(path.Join(prefix, "/api/v1/query")).Methods("GET", "POST").Handler(promRouter)
	router.Path(path.Join(prefix, "/api/v1/query_range")).Methods("GET", "POST").Handler(wrap(qapi.QueryRange))
	router.Path(path.Join(prefix, "/api/v1/query_exemplars")).Methods("GET", "POST").Handler(promRouter)
	router.Path(path.Join(prefix, "/api/v1/labels")).Methods("GET", "POST").Handler(promRouter)
	router.Path(path.Join(prefix, "/api/v1/label/{name}/values")).Methods("GET").Handler(promRouter)
	router.Path(path.Join(prefix, "/api/v1/series")).Methods("GET", "POST", "DELETE").Handler(promRouter)
	router.Path(path.Join(prefix, "/api/v1/metadata")).Methods("GET").Handler(promRouter)
	router.Path(path.Join(prefix, "/api/v1/status/buildinfo")).Methods("GET").Handler(promRouter)

	// TODO(gotjosh): This custom handler is temporary until we're able to vendor the changes in:
	// https://github.com/prometheus/prometheus/pull/7125/files
	router.Path(path.Join(legacyPrefix, "/api/v1/metadata")).Handler(querier.MetadataHandler(distributor))
	router.Path(path.Join(legacyPrefix, "/api/v1/read")).Handler(querier.RemoteReadHandler(queryable, logger))
	router.Path(path.Join(legacyPrefix, "/api/v1/read")).Methods("POST").Handler(legacyPromRouter)
	router.Path(path.Join(legacyPrefix, "/api/v1/query")).Methods("GET", "POST").Handler(legacyPromRouter)
	router.Path(path.Join(legacyPrefix, "/api/v1/query_range")).Methods("GET", "POST").Handler(legacyPromRouter)
	router.Path(path.Join(legacyPrefix, "/api/v1/query_exemplars")).Methods("GET", "POST").Handler(legacyPromRouter)
	router.Path(path.Join(legacyPrefix, "/api/v1/labels")).Methods("GET", "POST").Handler(legacyPromRouter)
	router.Path(path.Join(legacyPrefix, "/api/v1/label/{name}/values")).Methods("GET").Handler(legacyPromRouter)
	router.Path(path.Join(legacyPrefix, "/api/v1/series")).Methods("GET", "POST", "DELETE").Handler(legacyPromRouter)
	router.Path(path.Join(legacyPrefix, "/api/v1/metadata")).Methods("GET").Handler(legacyPromRouter)
	router.Path(path.Join(legacyPrefix, "/api/v1/status/buildinfo")).Methods("GET").Handler(legacyPromRouter)

	// Track execution time.
	return stats.NewWallTimeMiddleware().Wrap(router)
}

type buildInfoHandler struct {
	logger log.Logger
}

type buildInfoResponse struct {
	Status string                `json:"status"`
	Data   *v1.PrometheusVersion `json:"data"`
}

func (h *buildInfoHandler) ServeHTTP(writer http.ResponseWriter, _ *http.Request) {
	infoResponse := buildInfoResponse{
		Status: "success",
		Data: &v1.PrometheusVersion{
			Version:   version.Version,
			Branch:    version.Branch,
			Revision:  version.Revision,
			BuildUser: version.BuildUser,
			BuildDate: version.BuildDate,
			GoVersion: version.GoVersion,
		},
	}
	output, err := json.Marshal(infoResponse)
	if err != nil {
		level.Error(h.logger).Log("msg", "marshal build info response", "error", err)
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	writer.WriteHeader(http.StatusOK)
	if _, err := writer.Write(output); err != nil {
		level.Error(h.logger).Log("msg", "write build info response", "error", err)
	}
}

func Respond(w http.ResponseWriter, data interface{}, warnings []error) {
	w.Header().Set("Content-Type", "application/json")
	if len(warnings) > 0 {
		w.Header().Set("Cache-Control", "no-store")
	}
	w.WriteHeader(http.StatusOK)

	resp := &response{
		Status: StatusSuccess,
		Data:   data,
	}
	for _, warn := range warnings {
		resp.Warnings = append(resp.Warnings, warn.Error())
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func RespondError(w http.ResponseWriter, apiErr *api.ApiError, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")

	var code int
	switch apiErr.Typ {
	case api.ErrorBadData:
		code = http.StatusBadRequest
	case api.ErrorExec:
		code = 422
	case api.ErrorCanceled, api.ErrorTimeout:
		code = http.StatusServiceUnavailable
	case api.ErrorInternal:
		code = http.StatusInternalServerError
	default:
		code = http.StatusInternalServerError
	}
	w.WriteHeader(code)

	_ = json.NewEncoder(w).Encode(&response{
		Status:    StatusError,
		ErrorType: apiErr.Typ,
		Error:     apiErr.Err.Error(),
		Data:      data,
	})
}

type response struct {
	Status    status        `json:"status"`
	Data      interface{}   `json:"data,omitempty"`
	ErrorType api.ErrorType `json:"errorType,omitempty"`
	Error     string        `json:"error,omitempty"`
	Warnings  []string      `json:"warnings,omitempty"`
}

type status string

const (
	StatusSuccess status = "success"
	StatusError   status = "error"
)
