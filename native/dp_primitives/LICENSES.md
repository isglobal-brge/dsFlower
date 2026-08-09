# Licenses and provenance

The dsFlower wrapper and C ABI in this directory are Apache-2.0.

The exact noise sampler in `src/discrete_gaussian.rs` is a minimal port of
OpenDP 0.15.1 commit `c34d3d04a8872a51af523d9a2244be6171173b7d`.
OpenDP is distributed under the MIT License. Its CKS20 sampler incorporates
separately attributed MIT- and Apache-2.0-licensed work. The notices below are
retained both here, for binary distributions, and at the top of the ported
source. The upstream source-file hashes used for the port are recorded in
`UPSTREAM.env`.

## OpenDP notice (MIT)

Copyright (c) 2022 President and Fellows of Harvard College

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## Incorporated Thomas Steinke notice (Apache-2.0)

Copyright 2020 Thomas Steinke

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Cargo dependencies are resolved by `Cargo.lock`, which records exact versions
and registry checksums. Release builds must use `--locked`.
`THIRD_PARTY_LICENSES.html` is generated from that locked graph with the pinned
`cargo-about` configuration and must accompany every distributed native
library.
