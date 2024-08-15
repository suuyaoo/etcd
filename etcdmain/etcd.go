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

package etcdmain

import (
	"fmt"
	"os"
	"runtime"
	"strings"

	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/pkg/osutil"
	"go.etcd.io/etcd/pkg/types"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type dirType string

var (
	dirMember = dirType("member")
	dirProxy  = dirType("proxy")
	dirEmpty  = dirType("empty")
)

func startEtcdOrProxyV2() {
	grpc.EnableTracing = false

	cfg := newConfig()
	defaultInitialCluster := cfg.ec.InitialCluster

	err := cfg.parse(os.Args[1:])
	lg := cfg.ec.GetLogger()
	if err != nil {
		if lg != nil {
			lg.Warn("failed to verify flags", zap.Error(err))
		}
		switch err {
		case embed.ErrUnsetAdvertiseClientURLsFlag:
			if lg != nil {
				lg.Warn("advertise client URLs are not set", zap.Error(err))
			}
		}
		os.Exit(1)
	}

	defer func() {
		logger := cfg.ec.GetLogger()
		if logger != nil {
			logger.Sync()
		}
	}()

	defaultHost, dhErr := (&cfg.ec).UpdateDefaultClusterFromName(defaultInitialCluster)
	if defaultHost != "" {
		if lg != nil {
			lg.Info(
				"detected default host for advertise",
				zap.String("host", defaultHost),
			)
		}
	}
	if dhErr != nil {
		if lg != nil {
			lg.Info("failed to detect default host", zap.Error(dhErr))
		}
	}

	if cfg.ec.Dir == "" {
		cfg.ec.Dir = fmt.Sprintf("%v.etcd", cfg.ec.Name)
		if lg != nil {
			lg.Warn(
				"'data-dir' was empty; using default",
				zap.String("data-dir", cfg.ec.Dir),
			)
		}
	}

	var stopped <-chan struct{}
	var errc <-chan error

	which := identifyDataDirOrDie(cfg.ec.GetLogger(), cfg.ec.Dir)
	if which != dirEmpty {
		if lg != nil {
			lg.Info(
				"server has been already initialized",
				zap.String("data-dir", cfg.ec.Dir),
				zap.String("dir-type", string(which)),
			)
		}
		switch which {
		case dirMember:
			stopped, errc, err = startEtcd(&cfg.ec)
		default:
			if lg != nil {
				lg.Panic(
					"unknown directory type",
					zap.String("dir-type", string(which)),
				)
			}
		}
	} else {
		shouldProxy := cfg.isProxy()
		if !shouldProxy {
			stopped, errc, err = startEtcd(&cfg.ec)
			if err != nil {
				if lg != nil {
					lg.Warn("failed to start etcd", zap.Error(err))
				}
			}
		}
	}

	if err != nil {
		if strings.Contains(err.Error(), "include") && strings.Contains(err.Error(), "--initial-cluster") {
			if lg != nil {
				lg.Warn("failed to start", zap.Error(err))
			}
			if cfg.ec.InitialCluster == cfg.ec.InitialClusterFromName(cfg.ec.Name) {
				if lg != nil {
					lg.Warn("forgot to set --initial-cluster?")
				}
			}
			if types.URLs(cfg.ec.AdvertisePeerUrls).String() == embed.DefaultInitialAdvertisePeerURLs {
				if lg != nil {
					lg.Warn("forgot to set --initial-advertise-peer-urls?")
				}
			}
			os.Exit(1)
		}
		if lg != nil {
			lg.Fatal("discovery failed", zap.Error(err))
		}
	}

	osutil.HandleInterrupts(lg)

	select {
	case lerr := <-errc:
		// fatal out on listener errors
		if lg != nil {
			lg.Fatal("listener failed", zap.Error(lerr))
		}
	case <-stopped:
	}

	osutil.Exit(0)
}

// startEtcd runs StartEtcd in addition to hooks needed for standalone etcd.
func startEtcd(cfg *embed.Config) (<-chan struct{}, <-chan error, error) {
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, nil, err
	}
	osutil.RegisterInterruptHandler(e.Close)
	select {
	case <-e.Server.ReadyNotify(): // wait for e.Server to join the cluster
	case <-e.Server.StopNotify(): // publish aborted from 'ErrStopped'
	}
	return e.Server.StopNotify(), e.Err(), nil
}

// identifyDataDirOrDie returns the type of the data dir.
// Dies if the datadir is invalid.
func identifyDataDirOrDie(lg *zap.Logger, dir string) dirType {
	names, err := fileutil.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return dirEmpty
		}
		if lg != nil {
			lg.Fatal("failed to list data directory", zap.String("dir", dir), zap.Error(err))
		}
	}

	var m, p bool
	for _, name := range names {
		switch dirType(name) {
		case dirMember:
			m = true
		case dirProxy:
			p = true
		default:
			if lg != nil {
				lg.Warn(
					"found invalid file under data directory",
					zap.String("filename", name),
					zap.String("data-dir", dir),
				)
			}
		}
	}

	if m && p {
		if lg != nil {
			lg.Fatal("invalid datadir; both member and proxy directories exist")
		}
	}
	if m {
		return dirMember
	}
	if p {
		return dirProxy
	}
	return dirEmpty
}

func checkSupportArch() {
	// TODO qualify arm64
	if runtime.GOARCH == "amd64" || runtime.GOARCH == "ppc64le" {
		return
	}
	// unsupported arch only configured via environment variable
	// so unset here to not parse through flag
	defer os.Unsetenv("ETCD_UNSUPPORTED_ARCH")
	if env, ok := os.LookupEnv("ETCD_UNSUPPORTED_ARCH"); ok && env == runtime.GOARCH {
		fmt.Printf("running etcd on unsupported architecture %q since ETCD_UNSUPPORTED_ARCH is set\n", env)
		return
	}

	fmt.Printf("etcd on unsupported platform without ETCD_UNSUPPORTED_ARCH=%s set\n", runtime.GOARCH)
	os.Exit(1)
}
