// Copyright 2026 The CocoonStack Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package e2bcompat

import (
	"encoding/json"
	"net/http"

	"k8s.io/apiserver/pkg/storage/names"
)

const (
	// maxBodyBytes caps a request body. The e2b bodies are small objects; the
	// cap keeps an unbounded upload off the claim path.
	maxBodyBytes = 1 << 20
	// tokenAnnotation carries the per-sandbox ownership credential the claim
	// returned. It mirrors the aggregated apiserver's annotation of the same
	// name, and is surfaced to the SDK as envdAccessToken.
	tokenAnnotation = "sandbox.cocoonstack.io/token"
	// namePrefix prefixes the Kubernetes object name of a compat claim, so a
	// sandbox created through this surface is recognizable in `kubectl get
	// sandboxes` and in node inventory.
	namePrefix = "e2b-"
)

// generateName returns a fresh Kubernetes-safe name for a compat claim. e2b
// clients do not name their sandboxes; the identity a caller sees back is the
// node-assigned claim id, not this name.
func generateName() string {
	return names.SimpleNameGenerator.GenerateName(namePrefix)
}

// writeJSON writes v as the response body with the given status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// The body is already committed by WriteHeader, so an encode failure can
	// only be logged by the caller's transport; there is no status left to set.
	_ = json.NewEncoder(w).Encode(v)
}

// writeError writes the e2b error envelope, which the SDK surfaces as the
// failure message.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, APIError{Code: int32(status), Message: msg})
}
