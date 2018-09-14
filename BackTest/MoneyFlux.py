import SkyNETSim
from MoneyFluxConfig import backtestconfig
from multiprocessing import Pool
import multiprocessing

sim = SkyNETSim.SkyNETSim(backtestconfig)
sim.Run()