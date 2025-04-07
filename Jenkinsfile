// pubsubplus-go-client
//
// Copyright 2021-2025 Solace Corporation. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// The following is an internal infrastructure file for building 
properties([
    buildDiscarder(logRotator(daysToKeepStr: '30', numToKeepStr: '10')),
])
currentBuild.rawBuild.getParent().setQuietPeriod(0)

library 'jenkins-pipeline-library@main'

/*
  Go Version examples:
    auto-v1.17.x: Latest patch of 1.17 release
    auto-v1.17.2: Specific patch of 1.17 release
    auto-v1.17.0: First release of 1.17 (Despite go versioning this as 1.17)
    auto-latest: Most recent patch version of latest minor release
    auto-previous: Most recent patch version of previous minor release
    auto-2previous: Most recent patch version of second last minor release

  Adoption of new versions into these may be delayed.
*/

builder.goapi([
  "buildCheckGoVer": 'auto-v1.17.x',
  "validationGoVer": 'auto-v1.17.x',
  "getTestPermutations": {
    List<List<String>> permutations = []
    for (platform in [builder.LINUX_ARM, builder.LINUX_X86_64, builder.DARWIN_X86_64,  builder.DARWIN_ARM]) {
      for (gover in ['auto-latest', 'auto-previous']) {
        permutations << [platform, gover]
      }
    }
    // run tests on the last stable Go version (1.22.4) for linux musl
    // See EBP-46
    // and this issue here - https://go-review.googlesource.com/c/go/+/600296
    permutations << [builder.LINUX_MUSL, 'auto-v1.22.4']
    return permutations
  }
]) 
