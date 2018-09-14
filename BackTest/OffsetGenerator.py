import SkyNETSim
from OffsetGenConfig import offsetconfig
from multiprocessing import Pool

sim = SkyNETSim.SkyNETSim(offsetconfig)

sim.Run()

