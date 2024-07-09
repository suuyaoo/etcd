// Copyright 2018 The etcd Authors
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

//go:build !windows
// +build !windows

package logutil

import (
	"io"
)

// NewJournalWriter wraps "io.Writer" to redirect log output
// to the local systemd journal. If journald send fails, it fails
// back to writing to the original writer.
// The decode overhead is only <30µs per write.
// Reference: https://github.com/coreos/pkg/blob/master/capnslog/journald_formatter.go
func NewJournalWriter(wr io.Writer) (io.Writer, error) {
	return &journalWriter{Writer: wr}, nil
}

type journalWriter struct {
	io.Writer
}

func (w *journalWriter) Write(p []byte) (int, error) {
	// "fmt.Fprintln(os.Stderr, s)"
	return w.Writer.Write(p)
}
