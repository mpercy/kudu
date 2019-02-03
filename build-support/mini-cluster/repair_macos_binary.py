#!/usr/bin/env python
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
################################################################################
# This script fixes broken deps in a macOS native binary build.
################################################################################

from relocate_binaries_for_mini_cluster.py import PAT_MACOS_LIB_EXCLUDE
from relocate_binaries_for_mini_cluster.py import get_dep_library_paths_macos
from relocate_binaries_for_mini_cluster.py import relocate_dep_path_macos
from relocate_binaries_for_mini_cluster.py import fix_rpath_macos

SOURCE_ROOT = os.path.join(os.path.dirname(__file__), "../..")
# Add the build-support dir to the system path so we can import kudu-util.
sys.path.append(os.path.join(SOURCE_ROOT, "build-support"))

from kudu_util import check_output, Colors, init_logging

def main():
  if len(sys.argv) < 2:
    print("Usage: %s target [target ...]" % (sys.argv[0], ))
    sys.exit(1)

  # Command-line arguments.
  targets = sys.argv[1:]
  for target in targets:
    deps = get_dep_library_paths_macos(target)
    for (dep_search_name, dep_src) in target_deps.iteritems():
      if PAT_MACOS_LIB_EXCLUDE.search(dep_search_name): continue
      relocate_dep_path_macos(target, dep_search_name)
      fix_rpath_macos(target)

if __name__ == "__main__":
  main()
