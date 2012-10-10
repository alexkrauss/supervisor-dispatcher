from supervisor_dispatcher import Dispatcher

import unittest
import sys

from supervisor.tests.base import DummySupervisorRPCNamespace

class DispatcherTestCase(unittest.TestCase):

    def setUp(self):
        self.children = [ ("host1", DummySupervisorRPCNamespace()),
                          ("host2", DummySupervisorRPCNamespace()) ]

        self.dispatcher = Dispatcher(self.children)


    def test_getProcessInfo(self):

        info = self.dispatcher.getProcessInfo('foo@host1')
        assert info['name'] ==  'foo@host1'
        assert info['statename'] == 'RUNNING'


    def test_getAllProcessInfo(self):

        infos = self.dispatcher.getAllProcessInfo()
        
        assert any(info['name'] == 'foo@host1' for info in infos)
        assert any(info['name'] == 'foo@host2' for info in infos)

    
    def test_startProcessGroup(self):

        result = self.dispatcher.startProcessGroup('foo')
        assert len(result) == 4
        assert [entry['name'] for entry in result] == ['foo_00@host1', 'foo_01@host1', 'foo_00@host2', 'foo_01@host2']


def test_suite():
    return unittest.findTestCases(sys.modules[__name__])

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')


