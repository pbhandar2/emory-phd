from unittest import main, TestCase

from keyuri.config.Config import BaseMTExperiment


class TestConfig(TestCase):
    def test_basic(self):
        base_mt_config = BaseMTExperiment()
        replay_config_list = base_mt_config.get_all_replay_config()
        assert len(replay_config_list) > 0, "No replay config generated."


if __name__ == '__main__':
    main()