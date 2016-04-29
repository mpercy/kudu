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

# - Find BREAKPAD_CLIENT (google_breakpad/client/linux/handler/exception_handler.h, libbreakpad_client.a)
# This module defines
#  BREAKPAD_CLIENT_INCLUDE_DIR, directory containing headers
#  BREAKPAD_CLIENT_STATIC_LIB, path to libbreakpad_client's static library
#  BREAKPAD_CLIENT_SHARED_LIB, path to libbreakpad_client's shared library

find_path(BREAKPAD_CLIENT_INCLUDE_DIR client/linux/handler/exception_handler.h
  PATH_SUFFIXES breakpad
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)
find_library(BREAKPAD_CLIENT_STATIC_LIB libbreakpad_client.a
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)
find_library(BREAKPAD_CLIENT_SHARED_LIB breakpad_client
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(BREAKPAD_CLIENT REQUIRED_VARS
  BREAKPAD_CLIENT_SHARED_LIB BREAKPAD_CLIENT_STATIC_LIB BREAKPAD_CLIENT_INCLUDE_DIR)
