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

#include "kudu/util/signal.h"

#include "kudu/util/logging.h"

namespace kudu {

void SetSignalHandler(int signal, SignalHandlerCallback handler) {
  struct sigaction act;
  act.sa_handler = handler;
  sigemptyset(&act.sa_mask);
  act.sa_flags = 0;
  PCHECK(sigaction(signal, &act, nullptr) == 0);
}

void IgnoreSignal(int signal) {
  SetSignalHandler(signal, SIG_IGN);
}

void ResetSignalHandlerToDefault(int signal) {
  SetSignalHandler(SIGPIPE, SIG_DFL);
}

void BlockSignal(int signal) {
  sigset_t signals;
  PCHECK(sigemptyset(&signals) == 0);
  PCHECK(sigaddset(&signals, signal) == 0);
  PCHECK(pthread_sigmask(SIG_BLOCK, &signals, nullptr) == 0);
}

bool ConsumePendingSignal(int signal) {
  sigset_t signals;
  PCHECK(sigemptyset(&signals) == 0);
  PCHECK(sigaddset(&signals, signal) == 0);
  siginfo_t info;
  struct timespec timeout = { .tv_sec = 0, .tv_nsec = 0 };
  int pending_signal;
  do {
    pending_signal = sigtimedwait(&signals, &info, &timeout);
  } while (pending_signal == -1 && errno == EINTR); // Retry if interrupted.
  if (pending_signal == signal) return true;
  PCHECK(pending_signal == -1 && errno == EAGAIN)
      << "pending signal = " << pending_signal << ", errno = " << errno;
  return false; // No signals pending.
}

void UnblockSignal(int signal) {
  sigset_t signals;
  PCHECK(sigemptyset(&signals) == 0);
  PCHECK(sigaddset(&signals, signal) == 0);
  PCHECK(pthread_sigmask(SIG_UNBLOCK, &signals, nullptr) == 0);
}

bool IsSignalBlocked(int signal) {
  sigset_t blocked_signals;
  PCHECK(pthread_sigmask(SIG_UNBLOCK, nullptr, &blocked_signals) == 0);
  int ret = sigismember(&blocked_signals, signal);
  PCHECK(ret == 1 || ret == 0) << ret;
  return (ret == 1) ? true : false;
}

bool IsSignalPending(int signal) {
  sigset_t signals;
  PCHECK(sigpending (&signals) == 0);
  int ret = sigismember(&signals, signal);
  PCHECK(ret == 1 || ret == 0) << ret;
  return (ret == 1) ? true : false;
}

// We unblock all signal masks since they are inherited.
void ResetAllSignalMasksToUnblocked() {
  sigset_t signals;
  PCHECK(sigfillset(&signals) == 0);
  PCHECK(pthread_sigmask(SIG_UNBLOCK, &signals, nullptr) == 0);
}

} // namespace kudu
