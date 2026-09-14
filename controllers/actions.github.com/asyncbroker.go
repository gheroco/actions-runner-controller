package actionsgithubcom

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/actions/actions-runner-controller/apis/actions.github.com/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type asyncBrokerStatus struct {
	Phase string `json:"phase"`
	Ready bool   `json:"ready"`
}

func asyncBrokerEnabled(runner *v1alpha1.EphemeralRunner) bool {
	return runner.Spec.Annotations["async.gheroco.dev/broker"] == "enabled"
}

// Endpoint and credentials are controller configuration, never workflow inputs
// or a URL annotation that could redirect a runner's JIT credential.
func asyncBrokerRequest(ctx context.Context, method, uid string, body any) (*asyncBrokerStatus, int, error) {
	endpoint, err := url.Parse(os.Getenv("ARC_ASYNC_BROKER_URL"))
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return nil, 0, errors.New("ARC_ASYNC_BROKER_URL must be a trusted HTTPS endpoint")
	}
	token, err := os.ReadFile(os.Getenv("ARC_ASYNC_BROKER_TOKEN_FILE"))
	if err != nil || len(bytes.TrimSpace(token)) < 32 {
		return nil, 0, errors.New("async broker controller token is unavailable")
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		return nil, 0, errors.New("cannot load broker trust store")
	}
	if ca := os.Getenv("ARC_ASYNC_BROKER_CA_FILE"); ca != "" {
		pem, e := os.ReadFile(ca)
		if e != nil || !roots.AppendCertsFromPEM(pem) {
			return nil, 0, errors.New("invalid async broker CA")
		}
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}
	defer transport.CloseIdleConnections()
	httpClient := &http.Client{Transport: transport, Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	endpoint.Path = strings.TrimSuffix(endpoint.Path, "/") + "/v1/runners/" + url.PathEscape(uid)
	var payload []byte
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			return nil, 0, err
		}
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, 0, err
	}
	request.Header.Set("Authorization", "Bearer "+string(bytes.TrimSpace(token)))
	request.Header.Set("Content-Type", "application/json")
	response, err := httpClient.Do(request)
	if err != nil {
		return nil, 0, errors.New("async broker request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, response.StatusCode, nil
	}
	var status asyncBrokerStatus
	if err := json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&status); err != nil {
		return nil, 0, errors.New("invalid async broker response")
	}
	return &status, response.StatusCode, nil
}

func (r *EphemeralRunnerReconciler) reconcileAsyncBroker(ctx context.Context, runner *v1alpha1.EphemeralRunner, secret *corev1.Secret) (ctrl.Result, error) {
	status, code, err := asyncBrokerRequest(ctx, http.MethodGet, string(runner.UID), nil)
	if err != nil {
		return ctrl.Result{}, err
	}
	if code == http.StatusNotFound {
		status, code, err = asyncBrokerRequest(ctx, http.MethodPut, string(runner.UID), map[string]string{"jitConfig": string(secret.Data[jitTokenKey])})
		if err != nil {
			return ctrl.Result{}, err
		}
	}
	if code == http.StatusConflict {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	if code != http.StatusOK {
		return ctrl.Result{}, fmt.Errorf("async broker returned HTTP %d", code)
	}
	original := runner.DeepCopy()
	runner.Status.Ready = status.Ready
	switch status.Phase {
	case "Starting":
		runner.Status.Phase = v1alpha1.EphemeralRunnerPhasePending
	case "Running":
		runner.Status.Phase = v1alpha1.EphemeralRunnerPhaseRunning
	case "Succeeded":
		runner.Status.Phase = v1alpha1.EphemeralRunnerPhaseSucceeded
		runner.Status.Ready = false
	case "Cancelled", "Failed", "Lost":
		runner.Status.Phase = v1alpha1.EphemeralRunnerPhaseFailed
		runner.Status.Ready = false
	default:
		return ctrl.Result{}, errors.New("unknown async broker session phase")
	}
	if original.Status.Phase != runner.Status.Phase || original.Status.Ready != runner.Status.Ready {
		if err := r.Status().Patch(ctx, runner, client.MergeFrom(original)); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}
