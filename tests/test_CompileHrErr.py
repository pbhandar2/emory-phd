from unittest import main, TestCase

from keyuri.analysis.CompileHrErr import CompileHrErr


class TestConfig(TestCase):
    def test_basic(self):
        compiler = CompileHrErr("./data/CompileHrEr/test")
        


if __name__ == '__main__':
    main()