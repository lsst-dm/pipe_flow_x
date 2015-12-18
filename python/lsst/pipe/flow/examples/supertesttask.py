from __future__ import absolute_import, division, print_function

from lsst.pipe.flow.workflow import WorkFlowSeqTask, WorkFlowParTask

from lsst.pipe.base.examples.test1task import Test1Task
from lsst.pipe.base.examples.test2task import Test2Task

#from .test2task import Test2Task
import lsst.pex.config as pexConfig


class SuperTestConfig(pexConfig.Config):
    """
    Config
    """
    minval = pexConfig.Field(
        dtype=int,
        doc="Min value",
        default=2,
    )

class SuperTestTask(WorkFlowSeqTask):
    """
    SuperTest
    """
    ConfigClass = SuperTestConfig
    _default_name = 'Super_Flow_1'

    def __init__(self, config=None, name=None, parent_task=None, log=None, activator=None):
        super(SuperTestTask, self).__init__(config, name, parent_task, log, activator)
        print('%s was initiated' % self.name)

        T1 = Test1Task(name='phase 1')
        T1.config.do_print = True
        T2 = Test2Task(name='phase 2')
        T3 = Test1Task(name='phase 3')

        self.link(T1, T2, T3)


class Super2Config(pexConfig.Config):
    """
    Config
    """
    minval = pexConfig.Field(
        dtype=int,
        doc="Min value",
        default=2,
    )

class Super2Task(WorkFlowSeqTask):
    """
    SuperTest
    """
    ConfigClass = Super2Config
    _default_name = 'Big_Task'

    def __init__(self, config=None, name=None, parent_task=None, log=None, activator=None):
        super(Super2Task, self).__init__(config, name, parent_task, log, activator)

        T1 = Test1Task(name='T1')

        T2 = Test2Task(name='T2')
        T3 = Test2Task(name='T3')
        T6 = Test2Task(name='T6')
        T8 = Test2Task(name='T8')
        T9 = Test2Task(name='T9')
        S1 = WorkFlowSeqTask(name = 'S1Task').link(Test1Task(name='T_a'), Test1Task(name='T_b'))

        S2 = WorkFlowSeqTask(name = 'S2Task').link(Test1Task(name='T4'), Test1Task(name='T5'))
        S3 = WorkFlowParTask(name = 'S3Task')

        S3.link(Test1Task(name='T10'), S2, Test1Task(name='T11'))


        self.link(S1, T1, T2, T3, S3,  T8 ,T9 )

