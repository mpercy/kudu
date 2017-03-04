// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <signal.h>

namespace kudu {

#if defined(__linux__)
typedef sighandler_t SignalHandlerCallback;
#else
typedef sig_t SignalHandlerCallback;
#endif

// Set a process-wide signal handler.
void SetSignalHandler(int signal, SignalHandlerCallback handler);

// Set the disposition of the specified signal to SIG_IGN.
void IgnoreSignal(int signal);

// Set the disposition of the specified signal to SIG_DFL.
void ResetSignalHandlerToDefault(int signal);

// Begin blocking the specified signal in the current thread and any threads
// spawned by the current thread.
void BlockSignal(int signal);

// Consume a pending (blocked) signal of the specified type. Does not block.
// Returns true if there was a signal pending, false if not.
bool ConsumePendingSignal(int signal);

// Stop blocking the specified signal in the current thread.
void UnblockSignal(int signal);

// Returns whether the given signal is blocked in the current thread.
bool IsSignalBlocked(int signal);

// Returns whether the given signal is pending in the current thread.
bool IsSignalPending(int signal);

// Unblock all signals in the current thread.
void ResetAllSignalMasksToUnblocked();

} // namespace kudu
