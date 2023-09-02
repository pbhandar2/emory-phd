from pathlib import Path 
from numpy import array as np_arr
from itertools import cycle
import matplotlib.pyplot as plt



class Plot:
    def __init__(
            self,
            fig_size=[10, 7]
    ) -> None:
        self.fig, self.ax = plt.subplots(figsize=fig_size)

        self.x_label_font_size = 16
        self.y_label_font_size = 16

        self.x_tick_font_size = 14
        self.y_tick_font_size = 14

        self.legend_font_size = 14
        self.marker_size = 12

        self.line_plot_alpha = 0.75

        self.line_style_arr = ["--d", "-.*", "-.^", "--8", "--p", "--X"]


    def line_plot(
            self, 
            x: list, 
            y: list,
            line_style: str, 
            label: str 
    ) -> None:
        try:
            ax = self.ax[0]
        except Exception:
            ax = self.ax 
        
        ax.plot(x, y, line_style, markersize=self.marker_size, label=label, alpha=self.line_plot_alpha)
    

    def multi_line_plot(
            self, 
            x: list, 
            y: list,
            legend_arr: list, 
            x_label: str, 
            y_label: str
    ) -> None:
        line_index = 0 
        for x, y in zip(x, y):
            self.line_plot(x, y, self.line_style_arr[line_index], legend_arr[line_index])
            line_index += 1 
        
        self.ax.set_xlabel(x_label, fontsize=self.x_label_font_size)
        self.ax.set_ylabel(y_label, fontsize=self.y_label_font_size)

        # We change the fontsize of minor ticks label 
        self.ax.tick_params(axis='both', which='major', labelsize=self.x_tick_font_size)
        self.ax.tick_params(axis='both', which='minor', labelsize=self.y_tick_font_size)


    def savefig(
            self, 
            path: Path
    ) -> None:
        self.ax.legend(fontsize=self.legend_font_size)
        plt.savefig(path)


    def __del__(self):
        plt.close(self.fig)