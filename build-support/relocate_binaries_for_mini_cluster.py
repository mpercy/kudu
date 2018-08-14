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
# This script makes Kudu release binaries relocatable for easy use by
# integration tests using a mini cluster. The resulting binaries should never
# be deployed to run an actual Kudu service, whether in production or
# development, because all security dependencies are copied from the build
# system and will not be updated if the operating system on the runtime host is
# patched.
################################################################################

from __future__ import print_function

import logging
import optparse
import os
import os.path
import re
import subprocess
import sys

from kudu_util import check_output, confirm_prompt, Colors, get_my_email, init_logging

# Constants.
LC_RPATH = 'LC_RPATH'
LC_LOAD_DYLIB = 'LC_LOAD_DYLIB'
KEY_CMD = 'cmd'
KEY_NAME = 'name'
KEY_PATH = 'path'

# Config keys.
BUILD_ROOT = 'build_root'
BUILD_BIN_DIR = 'build_bin_dir'
ARTIFACT_ROOT = 'artifact_root'
ARTIFACT_BIN_DIR = 'artifact_bin_dir'
ARTIFACT_LIB_DIR = 'artifact_lib_dir'

SOURCE_ROOT = os.path.join(os.path.dirname(__file__), "..")
TARGETS = ["kudu-tserver", "kudu-master"]

IS_MACOS = os.uname()[0] == "Darwin"
IS_LINUX = os.uname()[0] == "Linux"

def objdump_private_headers(binary_path):
    """
    Run `objdump -p` on the given binary.
    Returns a list with one line of objdump output per record.
    """
    try:
        output = subprocess.check_output(["objdump", "-p", binary_path])
    except subprocess.CalledProcessError as err:
        print(err, file=sys.stderr)
        return []
    return output.strip().decode("utf-8").split("\n")

def parse_objdump_macos(cmd_type, dump):
    """
    Parses the output from objdump_private_headers() for macOS.
    'cmd_type' must be one of the following:
    * LC_RPATH: Returns a list containing the rpath search path, with one
      search path per entry.
    * LC_LOAD_DYLIB: Returns a list of shared object library dependencies, with
      one shared object per entry. They are returned as stored in the MachO
      header, without being first resolved to an absolute path, and may look
      like: @rpath/Foo.framework/Versions/A/Foo
    'dump' is the output from objdump_private_headers().
    """
    # Parsing state enum values.
    PARSING_NONE = 0
    PARSING_NEW_RECORD = 1
    PARSING_RPATH = 2
    PARSING_LIB_PATHS = 3

    state = PARSING_NONE
    values = []
    for line in dump:
        if re.match('^Load command', line):
            state = PARSING_NEW_RECORD
            continue
        splits = re.split('\s+', line.strip().decode("utf-8"), maxsplit=2)
        key = splits[0]
        val = splits[1] if len(splits) > 1 else None
        if state == PARSING_NEW_RECORD:
            if key == KEY_CMD and val == LC_RPATH:
                state = PARSING_RPATH
                continue
            if key == KEY_CMD and val == LC_LOAD_DYLIB:
                state = PARSING_LIB_PATHS
                continue

        if state == PARSING_RPATH and cmd_type == LC_RPATH:
            if key == KEY_PATH:
                # Strip trailing metadata from rpath dump line.
                values.append(val)

        if state == PARSING_LIB_PATHS and cmd_type == LC_LOAD_DYLIB:
            if key == KEY_NAME:
                values.append(val)
    return values

def get_rpaths_macos(binary_path):
    """
    Helper function that returns a list of rpaths parsed from the given binary.
    """
    dump = objdump_private_headers(binary_path)
    return parse_objdump_macos(LC_RPATH, dump)

def resolve_library_paths_macos(raw_library_paths, rpaths):
    """
    Resolve the library paths from parse_objdump_macos(LC_LOAD_DYLIB, ...) to
    absolute filesystem paths using the rpath information returned from
    get_rpaths_macos().
    Returns a mapping from original to resolved library paths on success.
    If any libraries cannot be resolved, prints an error to stderr and returns
    an empty map.
    """
    # TODO(mpercy): Throw an exception on error.
    resolved_paths = {}
    for raw_lib_path in raw_library_paths:
        if not raw_lib_path.startswith("@rpath"):
            resolved_paths[raw_lib_path] = raw_lib_path
            continue
        resolved = False
        for rpath in rpaths:
            resolved_path = re.sub('@rpath', rpath, raw_lib_path)
            if os.path.exists(resolved_path):
                resolved_paths[raw_lib_path] = resolved_path
                resolved = True
                break
        if not resolved:
            print("Unable to resolve %s with rpath %s" % (raw_lib_path, rpaths), file=sys.stderr)
            return {}
    return resolved_paths

def get_dep_library_paths_macos(binary_path):
    """
    Returns a map of symbolic to resolved library dependencies of the given binary.
    See resolve_library_paths_macos().
    """
    dump = objdump_private_headers(binary_path)
    raw_library_paths = parse_objdump_macos(LC_LOAD_DYLIB, dump)
    rpaths = parse_objdump_macos(LC_RPATH, dump)
    return resolve_library_paths_macos(raw_library_paths, rpaths)

def get_dep_library_paths_linux(binary_path):
    """
    Returns a list of library dependencies for the given binary using 'ldd'.
    """
    try:
        output = subprocess.check_output(["ldd", binary_path])
    except subprocess.CalledProcessError as err:
        print(err, file=sys.stderr)
        return []
    lines = output.strip().decode("utf-8").split("\n")
    libs = []
    for line in lines:
        components = re.split('\s+', line.strip(), maxsplit=4)
        if len(components) >= 3:
            libs.append(components[3])
    return libs

def get_artifact_name():
    """
    Read the Kudu version to create an archive with an appropriate name.
    """
    with open(os.path.join(SOURCE_ROOT, "version.txt"), 'r') as version:
        version = version.readline().strip().decode("utf-8")
    artifact_name = "apache-kudu-%s" % (version, )
    return artifact_name

def mkconfig(build_root, artifact_root):
    """
    Build a configuration map for convenient plumbing of path information.
    """
    config = {}
    config[BUILD_ROOT] = build_root
    config[BUILD_BIN_DIR] = os.path.join(build_root, "bin")
    config[ARTIFACT_ROOT] = artifact_root
    config[ARTIFACT_BIN_DIR] = os.path.join(artifact_root, "bin")
    config[ARTIFACT_LIB_DIR] = os.path.join(artifact_root, "lib")
    return config

def prep_artifact_dirs(config):
    """
    Create any required artifact output directories, if needed.
    """
    if not os.path.exists(config[ARTIFACT_BIN_DIR]):
        os.makedirs(config[ARTIFACT_BIN_DIR], mode=0755)
    if not os.path.exists(config[ARTIFACT_LIB_DIR]):
        os.makedirs(config[ARTIFACT_LIB_DIR], mode=0755)

def copy_executable(target, config):
    """
    Copy the executable identified by 'target' from the build directory to the
    artifact directory, and chmod it as executable.
    """
    target_src = os.path.join(config[BUILD_BIN_DIR], target)
    target_dst = os.path.join(config[ARTIFACT_BIN_DIR], target)
    subprocess.check_call(['cp', target_src, target_dst])
    os.chmod(target_dst, 0755)

def copy_library(src, dest):
    """
    Copy the library with path 'src' to path 'dest', and chmod it as read-write
    for the owner and read-only for everyone else.
    """
    subprocess.check_call(['cp', src, dest])
    os.chmod(dest, 0644)

def relocate_deps_linux(target, config):
    """
    See relocate_deps(). Linux implementation.
    """
    target_src = os.path.join(config[BUILD_BIN_DIR], target)
    target_dst = os.path.join(config[ARTIFACT_BIN_DIR], target)

    # Copy the linked libraries.
    PAT_LINUX_LIB_EXCLUDE = '(libpthread|libc|librt|libdl|libgcc.*)\.so'
    libs = []
    for lib in get_dep_library_paths_linux(target_src):
        if re.search(PAT_LINUX_LIB_EXCLUDE, lib):
            continue
        libs.append(lib)
    for lib_src in libs:
        lib_dst = os.path.join(config[ARTIFACT_LIB_DIR], os.path.basename(lib_src))
        copy_library(resolved_path, lib_dst)

    # Update the rpath.
    subprocess.check_call(['chrpath', '-r', '$ORIGIN/../lib', target_dst])

def relocate_deps_macos(target, config):
    """
    See relocate_deps(). macOS implementation.
    """
    target_src = os.path.join(config[BUILD_BIN_DIR], target)
    target_dst = os.path.join(config[ARTIFACT_BIN_DIR], target)

    PAT_MACOS_LIB_EXCLUDE = 'Kerberos|libSystem|libncurses'
    libs = get_dep_library_paths_macos(target_src)
    if not libs:
        print("Failed to resolve libraries for target %s" % target_src, file=sys.stderr)
        sys.exit(1)
    for (search_name, resolved_path) in libs.iteritems():
        # Filter out libs we don't want to archive.
        if re.search(PAT_MACOS_LIB_EXCLUDE, resolved_path):
            continue

        # Archive the rest of the runtime dependencies.
        lib_dst = os.path.join(config[ARTIFACT_LIB_DIR], os.path.basename(resolved_path))
        copy_library(resolved_path, lib_dst)

        # Change library search path or name for each archived library.
        modified_search_name = re.sub('^.*/', '@rpath/', search_name)
        subprocess.check_call(['install_name_tool', '-change',
                              search_name, modified_search_name, target_dst])
    # Modify the rpath.
    rpaths = get_rpaths_macos(target_src)
    for rpath in rpaths:
        subprocess.check_call(['install_name_tool', '-delete_rpath', rpath, target_dst])
    subprocess.check_call(['install_name_tool', '-add_rpath', '@executable_path/../lib',
                          target_dst])

def relocate_deps(target, config):
    """
    Copy all dependencies of the executable referenced by 'target' from the
    build directory into the artifact directory, and change the rpath of the
    executable so that the copied dependencies will be located when the
    executable is invoked.
    """
    if IS_MACOS:
        return relocate_deps_macos(target, config)
    elif IS_LINUX:
        return relocate_deps_linux(target, config)
    else:
        raise "Unsupported platform"

def main():
    if len(sys.argv) != 2:
        print("Usage: %s kudu_build_dir" % (sys.argv[0], ), file=sys.stderr)
        sys.exit(1)

    build_root = sys.argv[1]
    if not os.path.exists(build_root):
        print("Error: Build directory %s does not exist" % (build_root, ), file=sys.stderr)
        sys.exit(1)

    artifact_name = get_artifact_name()
    artifact_root = os.path.join(build_root, artifact_name)
    if not os.path.exists(artifact_root):
        os.makedirs(artifact_root, mode=0755)

    config = mkconfig(build_root, artifact_root)
    print("Including built dependencies in archive...")
    for target in TARGETS:
        prep_artifact_dirs(config)
        copy_executable(target, config)
        relocate_deps(target, config)

if __name__ == "__main__":
    main()
