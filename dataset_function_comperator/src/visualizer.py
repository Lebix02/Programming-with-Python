from bokeh.plotting import figure, show, output_file
from .tables import TableFunction, TablePoints
import pandas as pd
from pathlib import Path

MODULE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = MODULE_DIR / "visualizations"

class TableVisualization:
    """
    Handles the visualization of functions and points in a graph with the bokeh package.
    Uses the colors: blue, red, green, pink, orange, yellow and purple.
    The Figure of the bokeh package is created with the instance of this class.
    To show the figure call the function show() for this class instance.
    """

    def __init__(self, file_name: str, visualization_title: str):
        """
        Initializes the class object for the respective visualization. 
        
        :param file_name: The name for the HTML file that is created to display the plot.
        :type file_name: str
        :param visualization_title: The name for the title of the plot.
        :type visualization_title: str
        :param visualization_title: The title of the visualization displayed for the user.
        :type visualization_title: str
        :raises TypeError: If either file_name or visualization_title are not strings. 
        :raises ValueError: If the file_name is the empty string or only consists of whitespaces.
        """
        # Check if visulization_name is a string.
        if not isinstance(visualization_title, str):
                raise TypeError("visualization_title must be string, got", type(visualization_title).__name__)
        # Check if file_name is a string.
        if not isinstance(file_name, str):
                raise TypeError("file_name must be string, got", type(file_name).__name__)
        if not file_name or file_name.isspace():
                raise ValueError("file_name must not be empty or whitespace only") 
        
        self._file_name = file_name
        self._title = visualization_title
        self._p = figure(title=visualization_title)
        self.__colors = ("blue", "red", "green", "pink", "orange", "yellow", "purple")
        self.__count = 0

    def _get_color(self):
        """
        Getter method for the color that is to be used in the visualization.
        Moves the color one up in the color list of this class until the end is reached. Then sets it back to the beginning.
        
        :return: The color that is to be used next in the visualization.
        :rtype: str
        """
        # If the color count reached the last index of the list: Set back to 0.
        if self.__count == 6:
            self.__count = 0
        # Else: Add one to the index count
        else:
            self.__count += 1 
        return self.__colors[self.__count]

    def _add_function(self, x_coords: pd.Series, y_coords: pd.Series):
        """
        Adds the function given in the parameters to the visualization.
   
        :param x_coords: A list of x coordinates for the function.
        :type x_coords: pandas Series
        :param y_coords: A list of y coordinates for the function.
        :type y_coords: pandas Series
        :raises TypeError: If x_coords or y_coords are not pandas Series.
        :raises ValueError: If x_coords and y_coords are not of the same length.
        :raises TypeError: If x_coords or y_coords do not contain only float values.
        """
        # Check if x_coords and y_coords are pandas Series.
        if not isinstance(x_coords, pd.Series):
                raise TypeError("x_coords must be pandas Series, got", type(x_coords).__name__)
        if not isinstance(y_coords, pd.Series):
                raise TypeError("y_coords must be pandas Series, got", type(y_coords).__name__)
        # Check if x_coords and y_coords have the same length.
        if len(x_coords) != len(y_coords):
                raise ValueError("The lists are not the same length. Got", len(x_coords), "and", len(y_coords))
        # Check if x_coords and y_coords only contain float values.
        if not pd.api.types.is_float_dtype(x_coords):
                raise TypeError("x_coords must contain only floats")
        if not pd.api.types.is_float_dtype(y_coords):
                raise TypeError("y_coords must contain only floats")

        self._p.line(x_coords, y_coords, color=self._get_color())

    def add_functions(self, table: TableFunction, only_functions: set[int]=None):
        """
        Adds all functions of a TableFunction to the visualization. If only specific functions should be displayed,
        their index can be added to only_functions.
        
        :param table: A table containing at least one function.
        :type table: TableFunction
        :param only_functions: A set containing the indices of the functions that specifically should be displayed.
                               Indices must refer to function columns (1..width-1)
        :type only_functions: set of int
        :raise TypeError: If table is not a TableFunction.
        :raise TypeError: If only_functions is not a set.
        :raise ValueError: If the table does not hold at least one function.
        :raise ValueError: If the set does not contain only integers.
        :raise ValueError: If set contains integers outside of the function index range.
        """
        # Check if table is a TableFunction.
        if not isinstance(table, TableFunction):
                raise TypeError("table must be TableFunction, got", type(table).__name__)
        # Check if only_functions is a set.
        if only_functions is not None and not isinstance(only_functions, set):
                raise TypeError("only_functions must be set, got", type(only_functions).__name__)
        # Check if the table holds at least one function.
        if table.get_width() < 2:
                raise ValueError("The table does contain fewer than 2 columns, which is not sufficient for a function.")
        # Check if only_functions holds only integers.
        if only_functions is not None and not all(isinstance(v, int) for v in only_functions):
                raise ValueError("The set must contain only integers")
        # Check if only_functions holds only integers inside the range of the table.
        if only_functions is not None and not all(0 < v < table.get_width() for v in only_functions):
                raise ValueError("The set must contain only integers between 1 and the width of the table -1.")
        
        # Loop over all functions in the given table.
        for x in range(1, table.get_width()):

            # Check if the function in the iteration should be skipped
            if only_functions is not None:
                if x in only_functions:
                    self._add_function(table.get_column(0), table.get_column(x))

            # Else case if all functions should be displayed
            else:
                self._add_function(table.get_column(0), table.get_column(x))

    def add_points(self, table: TablePoints, only_mapping: set[str]=None):
        """
        Adds all points of a TablePoints to the visualization. If only specific points should be displayed,
        that have been mapped to specific functions, their names can be added to only_mapping.
        
        :param table: A table containing the points.
        :type table: TablePoints
        :param only_mapping: A set containing the mapping to specific functions by name.
        :type only_mapping: set of str
        :raise TypeError: If table is not a TablePoints.
        :raise TypeError: If only_mapping is not a set.
        :raise ValueError: If only_mapping does not contain only str.
        :raise ValueError: If only_mapping is given but TablePoints has fewer than 4 columns (no mapping present).
        """
        # Check if table is a TableClass.
        if not isinstance(table, TablePoints):
                raise TypeError("table must be TablePoints, got", type(table).__name__)
        # Check if only_mapping is a set.
        if only_mapping is not None and not isinstance(only_mapping, set):
                raise TypeError("only_mapping must be set, got", type(only_mapping).__name__)
        # Check if only_mapping holds only strings.
        if only_mapping is not None and not all(isinstance(v, str) for v in only_mapping):
                raise ValueError("The set must contain only strings")
        # Check if the table has the necessary 4 columns in the case that only_mapping is not None.
        if only_mapping is not None and table.get_width() < 4:
                raise ValueError("The table does contain fewer than 4 columns, which is not sufficient for the points,"
                "if a specific mapping should be displayed.")
        
        # Loop over all points in the given table.
        for x in range(0, table._get_length()):
            
            # Check if only a specific mapping should be displayed.
            if only_mapping is not None:
                # Check if the current point is mapped to the right function.
                if table._get_point(x, 3) in only_mapping: 
                    self._p.scatter(
                        table._get_point(x, 0),
                        table._get_point(x, 1),
                        marker="circle",
                        size=5,
                        color=self._get_color())
        
            # Else case if the mapping is irrelevant
            else:
                self._p.scatter(
                    table._get_point(x, 0),
                    table._get_point(x, 1),
                    marker="circle",
                    size=5,
                    color=self._get_color())

    def show(self):
        """
        Shows the visualization of this class instance with all the added information.
        """

        filename = f"{self._file_name}.html"
        filepath = OUTPUT_DIR / filename
        output_file(filepath, title=self._title)
        show(self._p)
    
    