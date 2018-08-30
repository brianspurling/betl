from configobj import ConfigObj
from .ExeClass import Exe
from .StateClass import State
from .ScheduleClass import Schedule
from .DataClass import Data
from .CtrlClass import Ctrl


#
# Conf() is a wrapper class to contain the "child" classes, making it easy
# to pass all the config around, whilst also keeping the code manageable
# TODO: probably separate out the methods from Data() so Data() is just the
# data config details, and the methods sit somewhere else.
#
class Conf():

    def __init__(self, appConfigFile, runTimeParams, scheduleConfig):

        # We use the ConfigObj package to process our appConfigFile, which is
        # divided up into sections corresponding to the "child" classes below
        self.allConfig = ConfigObj(appConfigFile)

        # Schedule config passed in from the CLI
        scheduleConfig['RUN_TESTS'] = runTimeParams['RUN_TESTS']

        # BETL's configuration is split into the following "child" classes
        self.EXE = Exe(runTimeParams)
        self.STATE = State()
        self.SCHEDULE = Schedule(scheduleConfig)
        self.DATA = Data(conf=self)
        self.CTRL = Ctrl(self.allConfig['ctrl'], self.EXE.RUN_RESET)

        # Finally, with the CTL DB initialised, we're able to init our
        # execution
        self.LAST_EXEC_REPORT = self.CTRL.initExecution(self.EXE, self.STATE)
