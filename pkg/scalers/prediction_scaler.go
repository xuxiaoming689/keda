package scalers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	tc "github.com/dysnix/predictkube-libs/external/types_convertation"
	"github.com/dysnix/predictkube-proto/external/proto/commonproto"
	"github.com/go-logr/logr"
	"github.com/go-playground/validator/v10"
	"github.com/prometheus/common/model"
	"k8s.io/api/autoscaling/v2beta2"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/metrics/pkg/apis/external_metrics"
	"net/http"
	"strconv"
	"strings"
	"time"

	kedautil "github.com/kedacore/keda/v2/pkg/util"
)

const (
	predictionMetricType   = "External"
	predictionMetricPrefix = "prediction_metric"

	invalidMetricsTypeErr = "metric type is invalid"
)

type PredictionScaler struct {
	metricType v2beta2.MetricTargetType
	metadata   *predictionMetadata
	httpClient *http.Client
	logger     logr.Logger
}

type predictionMetadata struct {
	predictionWindowSeconds int64
	triggerMode             string
	prometheusAddress       string
	predictAddress          string
	query                   string
	threshold               float64
	activationThreshold     float64
	scalerIndex             int
}

type predictResponse struct {
	Data    []float64 `json:"data"`
	Success int64     `json:"success"`
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

	//httpClient := kedautil.CreateHTTPClient(config.GlobalHTTPTimeout, false)
	// 10min for http client timeout
	httpClient := kedautil.CreateHTTPClient(600 * 1000 * time.Millisecond, false)
	s.httpClient = httpClient

	return s, nil
}

// IsActive returns true if we are able to get metrics from Prediction
func (s *PredictionScaler) IsActive(ctx context.Context) (bool, error) {
	print("start to trigger isActive")
	results, err := s.doQuery(ctx)
	if err != nil {
		return false, err
	}

	if s.metadata.triggerMode == "Preview" || results < 0 {
		return false, nil
	}

	return results > s.metadata.activationThreshold, nil
}

func (s *PredictionScaler) Close(_ context.Context) error {
	return nil
}

func (s *PredictionScaler) GetMetricSpecForScaling(context.Context) []v2beta2.MetricSpec {
	metricName := kedautil.NormalizeString(fmt.Sprintf("prediction-%s", predictionMetricPrefix))
	s.logger.V(0).Info("GetMetricSpecForScaling the metricName %s", metricName)
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
		s.logger.V(0).Error(err, "")
		return nil, err
	}

	s.logger.V(0).Info(fmt.Sprintf("predict value is: %f", value))

	metric := GenerateMetricInMili(metricName, value)

	return append([]external_metrics.ExternalMetricValue{}, metric), nil
}

func (s *PredictionScaler) doPredictRequest(ctx context.Context) (float64, error) {
	s.logger.V(0).Info("doPredictRequest ----> start")
	results, err := s.doQuery(ctx)
	if err != nil {
		return 0, err
	}
	s.logger.V(0).Info("doPredictRequest ----> end")
	return results, nil

}

func (s *PredictionScaler) doQuery(ctx context.Context) (float64, error) {
	url := fmt.Sprintf("%s/predict", s.metadata.predictAddress)
	query := s.metadata.query
	newQuery := strings.Replace(query, "\"", "%s", -1)
	var requestJSON = []byte(`{"query": "` + newQuery + `", "prometheusAddress": "` + s.metadata.prometheusAddress + `", "predictionWindowSeconds": "` + strconv.FormatInt(s.metadata.predictionWindowSeconds, 10) + `"}`)
	s.logger.V(0).Info(fmt.Sprintf("Start to predict, %s", s.metadata.query))
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
	if resp.StatusCode != http.StatusOK {
		s.logger.V(0).Info(fmt.Sprintf("The response of predict url failed, status code %d, %v", resp.StatusCode, err))
		return -1, err
	}
	var predictResp predictResponse
	if err := json.NewDecoder(resp.Body).Decode(&predictResp); err != nil {
		return -1, err
	}
	if predictResp.Success != 0 {
		// 服务端处理失败
		s.logger.V(0).Info(fmt.Sprintf("get predict response error ,code:%d", predictResp.Success))
	}
	var max float64
	if len(predictResp.Data) > 0 {
		max = predictResp.Data[0]
		for _, num := range predictResp.Data {
			if num > max {
				max = num
			}
		}
		s.logger.V(0).Info(fmt.Sprintf("The max predict value:%f", max))
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
