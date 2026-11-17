package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestPromMiddlewareCountsEveryRequestByItsRealStatusCode(t *testing.T) {
	handler := promMiddleware("/test-metrics", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("case") {
		case "redirect":
			http.Redirect(w, r, "https://example.com/file.pdf", http.StatusFound)
		case "missing":
			http.Error(w, "not found", http.StatusNotFound)
		default:
			_, _ = w.Write([]byte("ok")) // never calls WriteHeader: the status is an implicit 200
		}
	})

	for _, c := range []string{"redirect", "missing", "ok", "ok"} {
		handler(httptest.NewRecorder(), httptest.NewRequest("GET", "/x?case="+c, nil))
	}

	for status, want := range map[string]float64{"302": 1, "404": 1, "200": 2, "500": 0} {
		if got := testutil.ToFloat64(httpRequestsTotal.WithLabelValues("/test-metrics", status)); got != want {
			t.Errorf("status %s: counted %v requests, want %v", status, got, want)
		}
	}
}
