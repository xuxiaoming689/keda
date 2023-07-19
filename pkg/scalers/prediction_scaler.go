package scalers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	libs "github.com/dysnix/predictkube-libs/external/configs"
	tc "github.com/dysnix/predictkube-libs/external/types_convertation"
	"github.com/dysnix/predictkube-proto/external/proto/commonproto"
	"github.com/go-logr/logr"
	"github.com/go-playground/validator/v10"
	"github.com/prometheus/common/model"
	"k8s.io/api/autoscaling/v2beta2"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/metrics/pkg/apis/external_metrics"

	kedautil "github.com/kedacore/keda/v2/pkg/util"
)

const (
	predictionMetricType   = "External"
	predictionMetricPrefix = "prediction_metric"

	invalidMetricsTypeErr = "metric type is invalid"
)

var (
	engineHost = "api.prediction.com"
	enginePort = 443

	defaultSteps = time.Minute * 5

	grpcConnf = &libs.GRPC{
		Enabled:       true,
		UseReflection: true,
		Compression: libs.Compression{
			Enabled: false,
		},
		Conn: &libs.Connection{
			Host:            engineHost,
			Port:            uint16(enginePort),
			ReadBufferSize:  50 << 20,
			WriteBufferSize: 50 << 20,
			MaxMessageSize:  50 << 20,
			Insecure:        false,
			Timeout:         time.Second * 15,
		},
		Keepalive: &libs.Keepalive{
			Time:    time.Minute * 5,
			Timeout: time.Minute * 5,
			EnforcementPolicy: &libs.EnforcementPolicy{
				MinTime:             time.Minute * 20,
				PermitWithoutStream: false,
			},
		},
	}
)

type PredictionScaler struct {
	metricType       v2beta2.MetricTargetType
	metadata         *predictionMetadata
	httpClient 		 *http.Client
	logger           logr.Logger
}

type predictionMetadata struct {
	predictionWindowSeconds     int64
	triggerMode  string
	prometheusAddress   string
	predictAddress string
	query               string
	threshold           float64
	activationThreshold float64
	scalerIndex         int
}

type predictResponse struct {
	data     []float64
	success  int64
}

// NewPredictionScaler creates a new Prediction scaler
func NewPredictionScaler(config *ScalerConfig) (*PredictionScaler, error) {
	s := &PredictionScaler{}

	logger := InitializeLogger(config, "Prediction_scaler")
	s.logger = logger

	metricType, err := GetMetricTargetType(config)
	if err != nil {
		logger.Error(err, "error getting scaler metric type")
		return nil, fmt.Errorf("error getting scaler metric type: %s", err)
	}
	s.metricType = metricType

	meta, err := parsePredictionMetadata(config)
	if err != nil {
		logger.Error(err, "error parsing Prediction metadata")
		return nil, fmt.Errorf("error parsing Prediction metadata: %3s", err)
	}

	s.metadata = meta

	httpClient := kedautil.CreateHTTPClient(config.GlobalHTTPTimeout, false)
	s.httpClient = httpClient

	return s, nil
}

// IsActive returns true if we are able to get metrics from Prediction
func (s *PredictionScaler) IsActive(ctx context.Context) (bool, error) {
	results, err := s.doQuery(ctx)
	if err != nil {
		return false, err
	}

	if s.metadata.triggerMode == "Preview" {
		return false, nil
	}

	return results > s.metadata.activationThreshold, nil
}

func (s *PredictionScaler) Close(_ context.Context) error {
	return nil
}

func (s *PredictionScaler) GetMetricSpecForScaling(context.Context) []v2beta2.MetricSpec {
	metricName := kedautil.NormalizeString(fmt.Sprintf("prediction-%s", predictionMetricPrefix))
	externalMetric := &v2beta2.ExternalMetricSource{
		Metric: v2beta2.MetricIdentifier{
			Name: GenerateMetricNameWithIndex(s.metadata.scalerIndex, metricName),
		},
		Target: GetMetricTargetMili(s.metricType, s.metadata.threshold),
	}

	metricSpec := v2beta2.MetricSpec{
		External: externalMetric, Type: predictionMetricType,
	}
	return []v2beta2.MetricSpec{metricSpec}
}

func (s *PredictionScaler) GetMetrics(ctx context.Context, metricName string, _ labels.Selector) ([]external_metrics.ExternalMetricValue, error) {
	value, err := s.doPredictRequest(ctx)
	if err != nil {
		s.logger.Error(err, "error executing query to predict controller service")
		return []external_metrics.ExternalMetricValue{}, err
	}

	if value == -1 {
		err = errors.New("empty response after predict request")
		s.logger.Error(err, "")
		return nil, err
	}

	s.logger.V(1).Info(fmt.Sprintf("predict value is: %f", value))

	metric := GenerateMetricInMili(metricName, value)

	return append([]external_metrics.ExternalMetricValue{}, metric), nil
}

func (s *PredictionScaler) doPredictRequest(ctx context.Context) (float64, error) {
	results, err := s.doQuery(ctx)
	if err != nil {
		return 0, err
	}

	return results, nil

}

func (s *PredictionScaler) doQuery(ctx context.Context) (float64, error) {
	var queuePredicted []float64
	url := fmt.Sprintf("%s/predict", s.metadata.predictAddress)
	var requestJSON = []byte(`{"query": "`+s.metadata.query+`", "prometheusAddress": "`+s.metadata.prometheusAddress+`", "predictionWindowSeconds": "`+ strconv.FormatInt(s.metadata.predictionWindowSeconds, 10) +`"}`)
	request, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(requestJSON))

	if err != nil {
		return -1, err
	}
	request.Header.Set("Content-Type", "application/json")
	resp, err := s.httpClient.Do(request)
	if err != nil {
		return -1, err
	}

	defer resp.Body.Close()
	var predictResp predictResponse
	if err := json.NewDecoder(resp.Body).Decode(&predictResp); err != nil {
		return -1, err
	}
	if resp.StatusCode == 200 && predictResp.success == 0 {
		queuePredicted = predictResp.data
	} else {
		return -1, fmt.Errorf("predict response error code : %d %d", resp.StatusCode, predictResp.success)
	}

	max := queuePredicted[0]
	for _, num := range queuePredicted {
		if num > max {
			max = num
		}
	}
	return max, nil
}

// parsePrometheusResult parsing response from prometheus server.
func (s *PredictionScaler) parsePrometheusResult(result model.Value) (out []*commonproto.Item, err error) {
	metricName := GenerateMetricNameWithIndex(s.metadata.scalerIndex, kedautil.NormalizeString(fmt.Sprintf("prediction-%s", predictionMetricPrefix)))
	switch result.Type() {
	case model.ValVector:
		if res, ok := result.(model.Vector); ok {
			for _, val := range res {
				t, err := tc.AdaptTimeToPbTimestamp(tc.TimeToTimePtr(val.Timestamp.Time()))
				if err != nil {
					return nil, err
				}

				out = append(out, &commonproto.Item{
					Timestamp:  t,
					Value:      float64(val.Value),
					MetricName: metricName,
				})
			}
		}
	case model.ValMatrix:
		if res, ok := result.(model.Matrix); ok {
			for _, val := range res {
				for _, v := range val.Values {
					t, err := tc.AdaptTimeToPbTimestamp(tc.TimeToTimePtr(v.Timestamp.Time()))
					if err != nil {
						return nil, err
					}

					out = append(out, &commonproto.Item{
						Timestamp:  t,
						Value:      float64(v.Value),
						MetricName: metricName,
					})
				}
			}
		}
	case model.ValScalar:
		if res, ok := result.(*model.Scalar); ok {
			t, err := tc.AdaptTimeToPbTimestamp(tc.TimeToTimePtr(res.Timestamp.Time()))
			if err != nil {
				return nil, err
			}

			out = append(out, &commonproto.Item{
				Timestamp:  t,
				Value:      float64(res.Value),
				MetricName: metricName,
			})
		}
	case model.ValString:
		if res, ok := result.(*model.String); ok {
			t, err := tc.AdaptTimeToPbTimestamp(tc.TimeToTimePtr(res.Timestamp.Time()))
			if err != nil {
				return nil, err
			}

			s, err := strconv.ParseFloat(res.Value, 64)
			if err != nil {
				return nil, err
			}

			out = append(out, &commonproto.Item{
				Timestamp:  t,
				Value:      s,
				MetricName: metricName,
			})
		}
	default:
		return nil, errors.New(invalidMetricsTypeErr)
	}

	return out, nil
}

func parsePredictionMetadata(config *ScalerConfig) (result *predictionMetadata, err error) {
	validate := validator.New()
	meta := predictionMetadata{}

	if val, ok := config.TriggerMetadata["query"]; ok {
		if len(val) == 0 {
			return nil, fmt.Errorf("no query given")
		}

		meta.query = val
	} else {
		return nil, fmt.Errorf("no query given")
	}

	if val, ok := config.TriggerMetadata["predictAddress"]; ok {
		if len(val) == 0 {
			return nil, fmt.Errorf("no predictAddress given")
		}

		meta.predictAddress = val
	} else {
		return nil, fmt.Errorf("no predictAddress given")
	}

	if val, ok := config.TriggerMetadata["prometheusAddress"]; ok {
		err = validate.Var(val, "url")
		if err != nil {
			return nil, fmt.Errorf("invalid prometheusAddress")
		}

		meta.prometheusAddress = val
	} else {
		return nil, fmt.Errorf("no prometheusAddress given")
	}

	if val, ok := config.TriggerMetadata["predictionWindowSeconds"]; ok {
		meta.predictionWindowSeconds, err = strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("predictionWindowSeconds parsing error %s", err.Error())
		}
	} else {
		return nil, fmt.Errorf("no predictionWindowSeconds given")
	}

	if val, ok := config.TriggerMetadata["triggerMode"]; ok {
		if len(val) == 0 {
			return nil, fmt.Errorf("no triggerMode given")
		}

		meta.triggerMode = "Auto"
	} else {
		return nil, fmt.Errorf("no triggerMode given")
	}

	if val, ok := config.TriggerMetadata["threshold"]; ok {
		meta.threshold, err = strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, fmt.Errorf("threshold parsing error %s", err.Error())
		}
	} else {
		return nil, fmt.Errorf("no threshold given")
	}

	meta.activationThreshold = 0
	if val, ok := config.TriggerMetadata["activationThreshold"]; ok {
		meta.activationThreshold, err = strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, fmt.Errorf("activationThreshold parsing error %s", err.Error())
		}
	}

	meta.scalerIndex = config.ScalerIndex

	return &meta, nil
}

//func (s *PredictionScaler) ping(ctx context.Context) (err error) {
//	_, err = s.api.Runtimeinfo(ctx)
//	return err
//}

// initPredictionPrometheusConn init prometheus client and setup connection to API
//func (s *PredictionScaler) initPredictionPrometheusConn(ctx context.Context) (err error) {
//	var roundTripper http.RoundTripper
//	// create http.RoundTripper with auth settings from ScalerConfig
//	if roundTripper, err = authentication.CreateHTTPRoundTripper(
//		authentication.FastHTTP,
//		s.metadata.prometheusAuth,
//	); err != nil {
//		s.logger.V(1).Error(err, "init Prometheus client http transport")
//		return err
//	}
//
//	if s.prometheusClient, err = api.NewClient(api.Config{
//		Address:      s.metadata.prometheusAddress,
//		RoundTripper: roundTripper,
//	}); err != nil {
//		s.logger.V(1).Error(err, "init Prometheus client")
//		return err
//	}
//
//	s.api = v1.NewAPI(s.prometheusClient)
//
//	return s.ping(ctx)
//}
