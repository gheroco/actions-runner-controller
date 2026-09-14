package actionsgithubcom

import (
	"context"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/actions/actions-runner-controller/apis/actions.github.com/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestAsyncBrokerRegistersAndMapsStatusWithoutPod(t *testing.T) {
	calls := []string{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+strings.Repeat("a", 32) {
			t.Error("missing controller authentication")
		}
		if r.URL.Path != "/v1/runners/uid" {
			t.Errorf("unexpected route %s", r.URL.Path)
		}
		calls = append(calls, r.Method)
		if r.Method == http.MethodGet {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"phase":"Running","ready":true}`))
	}))
	defer server.Close()
	dir := t.TempDir()
	token := filepath.Join(dir, "token")
	ca := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(token, []byte(strings.Repeat("a", 32)), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ARC_ASYNC_BROKER_URL", server.URL)
	t.Setenv("ARC_ASYNC_BROKER_TOKEN_FILE", token)
	t.Setenv("ARC_ASYNC_BROKER_CA_FILE", ca)
	scheme := runtime.NewScheme()
	v1alpha1.AddToScheme(scheme)
	corev1.AddToScheme(scheme)
	runner := &v1alpha1.EphemeralRunner{ObjectMeta: metav1.ObjectMeta{Name: "runner", Namespace: "test", UID: types.UID("uid")}}
	runner.Spec.Annotations = map[string]string{"async.gheroco.dev/broker": "enabled"}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(runner).WithObjects(runner).Build()
	r := &EphemeralRunnerReconciler{Client: c}
	result, err := r.reconcileAsyncBroker(context.Background(), runner, &corev1.Secret{Data: map[string][]byte{jitTokenKey: []byte("synthetic-jit")}})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 || runner.Status.Phase != v1alpha1.EphemeralRunnerPhaseRunning || !runner.Status.Ready {
		t.Fatalf("unexpected runner status %+v", runner.Status)
	}
	if strings.Join(calls, ",") != "GET,PUT" {
		t.Fatal(calls)
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 0 {
		t.Fatal("created a runner pod in broker mode")
	}
}

func TestAsyncBrokerRefusesPlaintextAndRequiresOptIn(t *testing.T) {
	t.Setenv("ARC_ASYNC_BROKER_URL", "http://untrusted.invalid")
	if _, _, err := asyncBrokerRequest(context.Background(), http.MethodGet, "uid", nil); err == nil {
		t.Fatal("accepted HTTP")
	}
	if asyncBrokerEnabled(&v1alpha1.EphemeralRunner{}) {
		t.Fatal("broker mode enabled by default")
	}
}
