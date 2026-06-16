# -*- coding: utf-8 -*-
"""SCHEDULE_TIMES 多时间点配置解析与向后兼容测试（Issue #1581 P0）。"""

import os
import unittest
from unittest.mock import patch

from src.config import Config


class ParseScheduleTimesTestCase(unittest.TestCase):
    """Config._parse_schedule_times 纯函数测试（不依赖环境）。"""

    def test_empty_returns_empty_list(self):
        self.assertEqual(Config._parse_schedule_times(None), [])
        self.assertEqual(Config._parse_schedule_times(""), [])
        self.assertEqual(Config._parse_schedule_times("   "), [])
        self.assertEqual(Config._parse_schedule_times(",, ,"), [])

    def test_parses_dedups_and_sorts(self):
        self.assertEqual(
            Config._parse_schedule_times("18:00,09:20,12:30,09:20"),
            ["09:20", "12:30", "18:00"],
        )

    def test_trims_whitespace_around_items(self):
        self.assertEqual(
            Config._parse_schedule_times(" 09:20 , 12:30 "),
            ["09:20", "12:30"],
        )

    def test_drops_invalid_entries(self):
        # 越界、宽度不对、非数字一律丢弃
        self.assertEqual(
            Config._parse_schedule_times("09:20,25:00,9:5,12:60,abc,18:00"),
            ["09:20", "18:00"],
        )

    def test_boundary_times_valid(self):
        self.assertEqual(
            Config._parse_schedule_times("00:00,23:59"),
            ["00:00", "23:59"],
        )

    def test_all_invalid_returns_empty(self):
        self.assertEqual(Config._parse_schedule_times("25:00,bad,99:99"), [])


class EffectiveScheduleTimesTestCase(unittest.TestCase):
    """向后兼容：effective_schedule_times 在未配置 SCHEDULE_TIMES 时退回单时间。"""

    def tearDown(self):
        Config.reset_instance()

    @patch("src.config.setup_env")
    @patch.object(Config, "_get_env_file_value", return_value=None)
    @patch.object(Config, "_parse_litellm_yaml", return_value=[])
    def _load(self, env, _mock_yaml, _mock_file, _mock_setup):
        # 屏蔽仓库根 .env，仅以 os.environ 为准
        with patch.dict(os.environ, env, clear=True):
            return Config._load_from_env()

    def test_unset_schedule_times_falls_back_to_single_default_time(self):
        config = self._load({"STOCK_LIST": "600519"})
        self.assertEqual(config.schedule_times, [])
        self.assertEqual(config.schedule_time, "18:00")
        self.assertEqual(config.effective_schedule_times, ["18:00"])

    def test_unset_schedule_times_respects_custom_single_time(self):
        config = self._load({"STOCK_LIST": "600519", "SCHEDULE_TIME": "09:30"})
        self.assertEqual(config.schedule_times, [])
        self.assertEqual(config.effective_schedule_times, ["09:30"])

    def test_schedule_times_overrides_single_time(self):
        config = self._load(
            {
                "STOCK_LIST": "600519",
                "SCHEDULE_TIME": "18:00",
                "SCHEDULE_TIMES": "12:30,09:20",
            }
        )
        self.assertEqual(config.schedule_times, ["09:20", "12:30"])
        self.assertEqual(config.effective_schedule_times, ["09:20", "12:30"])

    def test_schedule_times_all_invalid_falls_back_to_single_time(self):
        config = self._load(
            {
                "STOCK_LIST": "600519",
                "SCHEDULE_TIME": "18:00",
                "SCHEDULE_TIMES": "25:00,bad",
            }
        )
        self.assertEqual(config.schedule_times, [])
        self.assertEqual(config.effective_schedule_times, ["18:00"])


if __name__ == "__main__":
    unittest.main()
