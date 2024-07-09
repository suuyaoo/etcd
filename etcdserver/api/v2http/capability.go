// Copyright 2015 The etcd Authors
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

package v2http

import (
	"fmt"
	"net/http"

	"go.etcd.io/etcd/etcdserver/api"
	"go.etcd.io/etcd/etcdserver/api/v2http/httptypes"
	"go.uber.org/zap"
)

func authCapabilityHandler(lg *zap.Logger, fn func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !api.IsCapabilityEnabled(api.AuthCapability) {
			notCapable(lg, w, r, api.AuthCapability)
			return
		}
		fn(w, r)
	}
}

func notCapable(lg *zap.Logger, w http.ResponseWriter, r *http.Request, c api.Capability) {
	herr := httptypes.NewHTTPError(http.StatusInternalServerError, fmt.Sprintf("Not capable of accessing %s feature during rolling upgrades.", c))
	if err := herr.WriteTo(w); err != nil {
		lg.Debug("error writing HTTPError", zap.Error(err), zap.String("remote addr", r.RemoteAddr))
	}
}
