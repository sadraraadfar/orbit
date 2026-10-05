package httpx

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/example/orbit/internal/platform/apierror"
	"github.com/example/orbit/internal/platform/correlation"
)

func TestWriteErrorShape(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req = req.WithContext(correlation.With(req.Context(), "corr-123"))
	rec := httptest.NewRecorder()

	WriteError(rec, req, apierror.NotFound("order not found"))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d", rec.Code)
	}
	var body ErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Code != apierror.CodeNotFound {
		t.Errorf("code = %s", body.Error.Code)
	}
	if body.Error.RequestID != "corr-123" {
		t.Errorf("request id = %q", body.Error.RequestID)
	}
}

func TestDecodeJSONRejectsUnknownFields(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{"known":"a","unknown":1}`))

	var dst struct {
		Known string `json:"known"`
	}
	err := DecodeJSON(rec, req, &dst)
	if err == nil {
		t.Fatal("expected error for unknown field")
	}
	apiErr := apierror.From(err)
	if apiErr.Code != apierror.CodeInvalidArgument {
		t.Fatalf("code = %s", apiErr.Code)
	}
}
