#     Copyright 2026. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

from platform import system
from unittest import TestCase, skipUnless
from unittest.mock import Mock, patch

from thingsboard_gateway.tb_utility import tb_memory_trimmer
from thingsboard_gateway.tb_utility.tb_memory_trimmer import TBMemoryTrimmer, MALLOC_TRIM_INTERVAL_ENV, \
    DEFAULT_MALLOC_TRIM_INTERVAL_SECONDS


class TestTBMemoryTrimmer(TestCase):

    def create_trimmer_with_mock(self, interval=10):
        malloc_trim = Mock(return_value=1)
        with patch.object(TBMemoryTrimmer, "_TBMemoryTrimmer__load_malloc_trim", return_value=malloc_trim):
            trimmer = TBMemoryTrimmer(interval=interval)
        return trimmer, malloc_trim

    def test_default_interval_when_env_not_set(self):
        with patch.dict(tb_memory_trimmer.environ, {}, clear=True):
            self.assertEqual(TBMemoryTrimmer().interval, DEFAULT_MALLOC_TRIM_INTERVAL_SECONDS)

    def test_interval_from_env(self):
        with patch.dict(tb_memory_trimmer.environ, {MALLOC_TRIM_INTERVAL_ENV: "15"}):
            self.assertEqual(TBMemoryTrimmer().interval, 15)

    def test_invalid_env_value_falls_back_to_default(self):
        with patch.dict(tb_memory_trimmer.environ, {MALLOC_TRIM_INTERVAL_ENV: "abc"}):
            self.assertEqual(TBMemoryTrimmer().interval, DEFAULT_MALLOC_TRIM_INTERVAL_SECONDS)

    def test_zero_interval_disables_trimming(self):
        with patch.dict(tb_memory_trimmer.environ, {MALLOC_TRIM_INTERVAL_ENV: "0"}):
            trimmer = TBMemoryTrimmer()
        self.assertFalse(trimmer.enabled)
        trimmer.trim_if_needed()

    def test_disabled_on_non_linux(self):
        with patch.object(tb_memory_trimmer, "system", return_value="Windows"):
            self.assertFalse(TBMemoryTrimmer(interval=10).enabled)

    def test_disabled_when_malloc_trim_is_missing(self):
        libc = Mock(spec=[])
        with patch.object(tb_memory_trimmer, "system", return_value="Linux"), \
                patch.object(tb_memory_trimmer.ctypes, "CDLL", return_value=libc):
            self.assertFalse(TBMemoryTrimmer(interval=10).enabled)

    def test_trims_only_when_interval_elapsed(self):
        with patch.object(tb_memory_trimmer, "monotonic", return_value=100):
            trimmer, malloc_trim = self.create_trimmer_with_mock(interval=10)

        with patch.object(tb_memory_trimmer, "monotonic", return_value=105):
            trimmer.trim_if_needed()
        malloc_trim.assert_not_called()

        with patch.object(tb_memory_trimmer, "monotonic", return_value=110):
            trimmer.trim_if_needed()
            trimmer.trim_if_needed()
        malloc_trim.assert_called_once_with(0)

        with patch.object(tb_memory_trimmer, "monotonic", return_value=120):
            trimmer.trim_if_needed()
        self.assertEqual(malloc_trim.call_count, 2)

    def test_failing_malloc_trim_disables_trimming(self):
        with patch.object(tb_memory_trimmer, "monotonic", return_value=100):
            trimmer, malloc_trim = self.create_trimmer_with_mock(interval=10)
        malloc_trim.side_effect = OSError("failure")

        with patch.object(tb_memory_trimmer, "monotonic", return_value=110):
            trimmer.trim_if_needed()
        self.assertFalse(trimmer.enabled)

    @skipUnless(system() == "Linux", "malloc_trim is available on Linux only")
    def test_real_malloc_trim_call(self):
        with patch.object(tb_memory_trimmer, "monotonic", return_value=100):
            trimmer = TBMemoryTrimmer(interval=1)
        if not trimmer.enabled:
            self.skipTest("malloc_trim is not provided by the C library")

        with patch.object(tb_memory_trimmer, "monotonic", return_value=101):
            trimmer.trim_if_needed()
        self.assertTrue(trimmer.enabled)
