""" BarSTDPlot plots groups of bar which represents the mean and error bars that represents the standard deviation of
the array of values it is representing. 
"""

import matplotlib.pyplot as plt
from matplotlib.axes import Axes

DEFAULT_FONT_SIZE = 37
DEFAULT_FIG_SIZE = [28, 10]

plt.rcParams.update({'font.size': DEFAULT_FONT_SIZE})



def plot_bar_std():
    pass 



class BarSTDPlot:
    def __init__(
            self,
            ax: Axes
    ) -> None:
        self.ax = ax 


    def _plot(self):
        pass 