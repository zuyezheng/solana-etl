from unittest import TestCase


class TestFileOutput(TestCase):

    def test_with_local_cluster(self):
        class Foo:

            def __init__(self, var_a, var_b):
                print(var_a, var_b)

        def foo_gen(**kwargs):
            kwargs['var_b'] = 'world'
            return Foo(**kwargs)

        foo_gen(var_a='hello')

        self.fail()
