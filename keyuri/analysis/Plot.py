"""A wrapper for matplotlib to create more complex plots.

Usage: 
    plot = Plot()
    plot.multi_lineplot(row_count, col_count, x_arr, y_arr)
"""

from pathlib import Path 

import matplotlib.pyplot as plt
from matplotlib.axes import Axes

DEFAULT_FONT_SIZE = 37
DEFAULT_FIG_SIZE = [28, 10]

plt.rcParams.update({'font.size': DEFAULT_FONT_SIZE})


class Plot:
    def __init__(
            self,
            row_count = 1, 
            col_count = 1, 
            fig_size = DEFAULT_FIG_SIZE,
            scaler: int = 5
    ) -> None:
        """Initiate the plot. 

        Args:
            row_count: Number of rows in the plot. 
            col_count: Number of columns in the plot. 
            fig_size: List comrpising of length, height per plot. 
        """
        self.fig, self.ax = plt.subplots(row_count, col_count, figsize=[fig_size[0]*row_count, fig_size[1]*col_count])
        self.scaler = row_count*col_count/scaler 

        self.x_label_font_size = 16
        self.y_label_font_size = 16

        self.x_tick_font_size = 14
        self.y_tick_font_size = 14

        self.legend_font_size = 14
        self.marker_size = 12

        self.line_plot_alpha = 0.75

        self.line_style_arr = ["--d", "-.*", "-.^", "--8", "--p", "--X"]
    

    def set_title(
            self, 
            title_str: str,
            plot_index: int = 0 
    ) -> None: 
        try:
            ax = self.ax.flatten()[plot_index]
        except Exception:
            ax = self.ax 
        
        ax.set_title(title_str, fontsize=24)
    

    def line_plot(
            self, 
            x: list, 
            y: list,
            line_style: str, 
            label: str,
            plot_index = 0
    ) -> None:
        """Plot a line. 

        Args:
            x: Array of x values. 
            y: Array of y values. 
            line_style: String line style comprising of line and marker style. 
            label: Label used for legend. 
            plot_index: The index of the plot in case there are multiple plots in this figure. 
        """
        try:
            ax = self.ax.flatten()[plot_index]
        except Exception:
            ax = self.ax 
        
        ax.plot(x, y, line_style, markersize=self.marker_size, label=label, alpha=self.line_plot_alpha)

        ax.set_xticks(x) 
        ax.tick_params(axis='both', which='major', labelsize=self.x_tick_font_size*self.scaler)
        ax.tick_params(axis='both', which='minor', labelsize=self.y_tick_font_size*self.scaler)
    

    def multi_line_plot(
            self, 
            x: list, 
            y: list,
            label_arr: list, 
            plot_index: int = 0 
    ) -> None:
        """Plot multiple lines in a plot. 

        Args:
            x: Array of x values. 
            y: Array of y values. 
            label_arr: Array of label values. 
            x_label: X label to be used in plot. 

            plot_index: The index of the plot in case there are multiple plots in this figure. 
        """
        line_index = 0 
        for x, y in zip(x, y):
            self.line_plot(x, y, self.line_style_arr[line_index], label_arr[line_index], plot_index=plot_index)
            line_index += 1 


    def set_legend(self, loc: str = "upper center", plot_index: int = 0):
        try:
            ax = self.ax.flatten()[plot_index]
        except Exception:
            ax = self.ax 
        handles, labels = ax.get_legend_handles_labels()
        self.fig.legend(handles, labels, loc='upper center', ncol=6, fontsize=self.legend_font_size*self.scaler, bbox_to_anchor=(0.5, 0.95))


    def savefig(
            self, 
            path: Path
    ) -> None:
        plt.savefig(path, bbox_inches='tight')


    def __del__(self):
        plt.close(self.fig)