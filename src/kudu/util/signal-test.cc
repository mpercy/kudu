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

#include <errno.h>
#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <memory>
#include <thread>

#include "kudu/gutil/macros.h"
#include "kudu/util/signal.h"
#include "kudu/util/test_util.h"

using std::atomic;
using std::thread;

namespace kudu {

// Signal handler to increment an atomic counter.
static atomic<int> g_signal_count;
static void IncrementCounter(int /*signal*/) {
  g_signal_count++;
}

class SignalTest : public KuduTest {
 public:
  void SetUp() override;
  void TearDown() override;

 protected:
  void StartSleeperThread();
  void StopSleeperThread();
  void SendSigPipeToProcess(pid_t pid);
  void SendSigPipeToCallerThread();
  atomic<bool> run_sleeper_thread_;
  thread sleeper_thread_;
};

void SignalTest::SetUp() {
  KuduTest::SetUp();
  ResetSignalHandlerToDefault(SIGPIPE);
  g_signal_count = 0; // Reset static counter.
}

void SignalTest::TearDown() {
  StopSleeperThread();
  KuduTest::TearDown();
}

void SignalTest::StartSleeperThread() {
  run_sleeper_thread_ = true;
  sleeper_thread_ = thread([&] {
    while (run_sleeper_thread_) {
      SleepFor(MonoDelta::FromMicroseconds(1));
    }
  });
}

void SignalTest::StopSleeperThread() {
  run_sleeper_thread_ = false;
  if (sleeper_thread_.joinable()) sleeper_thread_.join();
}

void SignalTest::SendSigPipeToProcess(pid_t pid) {
  // Spawn a new thread to send a PIPE signal to the current process.
  // We do this because if the thread sending the signal has the signal
  // blocked, it will go into pending state on that thread.
  thread t([pid] { PCHECK(kill(pid, SIGPIPE) == 0); });
  t.join();
}

void SignalTest::SendSigPipeToCallerThread() {
  int pipefd[2];
  ASSERT_EQ(0, pipe(pipefd)) << errno;
  ASSERT_EQ(0, close(pipefd[0])) << errno;
  const char kStr[] = "HELLO\n";
  ASSERT_EQ(-1, write(pipefd[1], kStr, arraysize(kStr))) << errno;
  int tmp_errno = errno; // Save the errno from write() so we can reset it.
  ASSERT_EQ(0, close(pipefd[1])) << errno; // Don't leak the pipe fd.
  errno = tmp_errno; // Restore the error errno from above.
}

TEST_F(SignalTest, TestIgnoreSignal) {
  IgnoreSignal(SIGPIPE);
  NO_FATALS(SendSigPipeToCallerThread());
  ASSERT_EQ(EPIPE, errno);
}

TEST_F(SignalTest, TestHandleSignal) {
  SetSignalHandler(SIGPIPE, &IncrementCounter);
  ASSERT_EQ(0, g_signal_count);
  NO_FATALS(SendSigPipeToCallerThread());
  ASSERT_EQ(EPIPE, errno);
  ASSERT_EQ(1, g_signal_count);
}

TEST_F(SignalTest, TestBlockSignal) {
  // Keep a thread running that simply sleeps to ensure that there is
  // always a thread that could handle a process-wide signal if it is
  // sent.
  StartSleeperThread();

  // Install a process-wide signal handler that should not be invoked while the
  // signal is blocked.
  SetSignalHandler(SIGPIPE, &IncrementCounter);

  ASSERT_EQ(0, g_signal_count);
  BlockSignal(SIGPIPE);
  NO_FATALS(SendSigPipeToCallerThread());
  ASSERT_EQ(EPIPE, errno);
  ASSERT_TRUE(ConsumePendingSignal(SIGPIPE));
  ASSERT_FALSE(ConsumePendingSignal(SIGPIPE)); // Already consumed.
  UnblockSignal(SIGPIPE);
  ASSERT_EQ(0, g_signal_count);

  // The global handler should now be invoked again
  NO_FATALS(SendSigPipeToCallerThread());
  ASSERT_EQ(1, g_signal_count);
}

TEST_F(SignalTest, TestIsSignalBlocked) {
  BlockSignal(SIGPIPE);
  ASSERT_TRUE(IsSignalBlocked(SIGPIPE));
  UnblockSignal(SIGPIPE);
  ASSERT_FALSE(IsSignalBlocked(SIGPIPE));
}

// Test that we can receive a process-wide signal even if a single thread
// blocks it.
TEST_F(SignalTest, TestProcessWideSignalSingleThreadBlocked) {
  StartSleeperThread();
  SetSignalHandler(SIGPIPE, &IncrementCounter);
  BlockSignal(SIGPIPE); // On current thread only.

  // Blocked but not pending.
  ASSERT_EQ(0, g_signal_count);
  SendSigPipeToProcess(getpid());
  ASSERT_EQ(1, g_signal_count);

  // Blocked and pending.
  NO_FATALS(SendSigPipeToCallerThread());
  SendSigPipeToProcess(getpid());
  int signal_count = g_signal_count;
  // Because the signal is already pending in a single thread, it is no longer
  // delivered process-wide.
  ASSERT_EQ(1, signal_count);
}

} // namespace kudu
