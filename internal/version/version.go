// Copyright 2026 The Kubernetes Authors.
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

// nolint:revive
package version

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
	"text/template"
)

var (
	gitVersion = "unknown"
	gitSHA     = "unknown"
	buildDate  = "unknown"

	goVersion  = runtime.Version()
	goCompiler = runtime.Compiler
	goOS       = runtime.GOOS
	goArch     = runtime.GOARCH

	versionInfoTmpl = `
{{.Program}}, version {{.GitVersion}} (revision: {{.GitSHA}})
  build date:       {{.BuildDate}}
  go version:       {{.GoVersion}}
  compiler:         {{.Compiler}}
  platform:         {{.Platform}}
`
)

type Info struct {
	Program    string `json:"program"`
	GitVersion string `json:"gitVersion"`
	GitSHA     string `json:"gitSHA"`
	BuildDate  string `json:"buildDate"`
	GoVersion  string `json:"goVersion"`
	Compiler   string `json:"compiler"`
	Platform   string `json:"platform"`
}

// Get returns version information populated with build-time values.
func Get() Info {
	return Info{
		GitVersion: gitVersion,
		GitSHA:     gitSHA,
		BuildDate:  buildDate,
		GoVersion:  goVersion,
		Compiler:   goCompiler,
		Platform:   goOS + "/" + goArch,
	}
}

// String returns a Go-syntax representation of the Info.
func (info Info) String() string {
	return fmt.Sprintf("%#v", info)
}

// Print returns version information.
func Print(program string) string {
	m := Get()
	m.Program = program
	t := template.Must(template.New("version").Parse(versionInfoTmpl))

	var buf bytes.Buffer
	if err := t.ExecuteTemplate(&buf, "version", m); err != nil {
		panic(err)
	}
	return strings.TrimSpace(buf.String())
}
