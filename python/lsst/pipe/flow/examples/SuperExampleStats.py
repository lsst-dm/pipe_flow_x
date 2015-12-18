from __future__ import absolute_import, division, print_function

from lsst.pipe.flow.workflow import WorkFlowSeqTask, WorkFlowParTask

from lsst.pipe.base.examples.ExampleStats import ExampleMeanTask
from lsst.pipe.base.examples.ExampleStats import ExampleStdTask
import lsst.pex.config as pexConfig


class AllStatConfig(pexConfig.Config):
    """
    Config
    """
    minval = pexConfig.Field(
        dtype=int,
        doc="Min value",
        default=2,
    )

class AllStatTask(WorkFlowSeqTask):
    """
    SuperTest
    """
    ConfigClass = AllStatConfig
    _default_name = 'All_Stats'

    def __init__(self, config=None, name=None, parent_task=None, log=None, activator=None):
        super(AllStatTask, self).__init__(config, name, parent_task, log, activator)

        Mean = ExampleMeanTask()
        Mean.config.numSigmaClip = 4.0

        Mean2 = ExampleMeanTask(name='mean 2nd value')
        Mean2.config.numSigmaClip = 5.0

        Std = ExampleStdTask()

        self.link(Mean, Mean2, Std)
