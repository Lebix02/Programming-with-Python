import pandas as pd
import sqlalchemy as sa
import math
import numpy as np
from pathlib import WindowsPath

class TableClass(object):
    """
    Parent class for TableClasses. 
    Safes a Table as a pandas DataFrame.
    Contains the generic get-methods and the sql handling, which are pased down to the specific child classes.
    """

    def __init__(self, table_name: str, file_path: WindowsPath=None, data: dict=None):
        """
        Initializes the class instance with the respective name for the table. 
        Either the file_name or the data are used to create a dataframe using the pandas extension, 
        therefore one of them must be given, when called.
        
        :param table_name: The name of the created table. Used for the sql handling. Can't be empty or only whitespaces.
        :type table_name: str
        :param file_path: The path of the csv-file that is to be read and converted into the dataframe for this class object. 
        It has to precisly copy the file path of the file.
        :type file_path: WindowsPath 
        :param data: A dictonary that holds one or multiple pandas series.
        :type data: dict
        :raises TypeError: If table_name or file_name are not a string.
        :raises ValueError: If the table_name or file_name parameter are given as an empty string or a string of spaces.
        :raises TypeError: If data is not a dictonary.
        :raises ValueError: If neither the file_name or the data parameter are given.
        :raises TypeError: If not all values in the table are a float dtype.
        """
        # Check if table_name is a valid string.
        if not isinstance(table_name, str):
                raise TypeError("table_name must be string, got", type(table_name).__name__)    
        if not table_name or table_name.isspace():
                raise ValueError("table_name must not be empty or whitespace only") 
        
        # Check if file_name is given and a string.
        if file_path is not None:
            if not isinstance(file_path, WindowsPath):
                raise TypeError("file_path must be WindowsPath, got", type(file_path).__name__)
            
            # Try finding the file, based on the given path.
            try:
                self.df = pd.read_csv(file_path)
            #Catch the error for not finding the file, most likely because the path is invalid.
            except FileNotFoundError:
                raise ValueError("Invalid file path")
            
        # Check if data is given and a dictonary.
        elif data is not None:
            if not isinstance(data, dict):
                raise TypeError("data must be dictonary, got", type(data).__name__)
            self.df = pd.DataFrame(data)
        
        # Raise Error if neither file_name or data are given. Need at least one!
        else: 
                raise ValueError("no parameters")
        
        # Raise Error if the given data for the dataframe does not only contain float values
        if not all(pd.api.types.is_float_dtype(dt) for dt in self.df.dtypes):
                raise TypeError("All columns must be float dtype.")
        
        if len(self.df.columns) < 2:
                raise ValueError("A table must contain at least two columns.")

        self.engine = sa.create_engine("sqlite:///:memory:")

        self.connection = self.engine.connect()
        self.sql_save_count = 0
        self.table_name = table_name

    def print_table(self):
        """
        Prints the table represented by this class instance in the terminal.
        """
        print(self.df)

    def get_column_names(self):
        """
        Getter method for a copy of the list of all column names of the table represented by this class instance.

        :return: A list containing the names of the columns of this table.
        :rtype: pandas series of strings
        """
        return self.df.columns.copy()

    def get_column(self, column: int):
        """
        Getter method for a copy of a whole column of the table represented by this class instance.
        
        :param column: The index of the column, that is called. Has to be inside of the tables width.
        :type column: int or int64
        :return: The column of this list at the given indice.
        :rtype: pandas series
        :raises TypeError: If column is neither an int or int64.
        :raises ValueError: If the column parameter is not inside of the tables width.
        """
        # Check if the column is valid.
        if not isinstance(column, (int, np.int64)):
                raise TypeError("column must be int or int64, got", type(column).__name__)
        if not 0 <= column < self.get_width():
                raise ValueError("column must be between 0 and", self.get_width()-1)
        
        return self.df.iloc[:, column].copy()
    
    def get_row(self, row: int):
        """
        Getter method for a copy of a whole row of the table represented by this class instance.
        
        :param row: The index of the row, that is called.
        :type row: int or int64
        :return: The row of this list at the given indice.
        :rtype: pandas series
        :raises TypeError: If row is neither an int or int64.
        :raises ValueError: If the row parameter is not inside of the tables length.
        """
        # Check if the row is valid.
        if not isinstance(row, (int, np.int64)):
                raise TypeError("row must be int or int64, got", type(row).__name__)
        if not 0 <= row < self.get_length():
                raise ValueError("row must be between 0 and", self.get_length()-1)
        
        return self.df.iloc[row].copy()
    
    def find_row(self, column: int, value: float):
        """
        Finds a row based on a specif value, that is searched in a specific column. 
        Then returns a copy of the whole row in which the value is held.
        
        :param column: The index of the column, that is called. Has to be inside of the tables width.
        :type column: int or int64
        :param value: The specific value that is searched.
        :type value: float
        :return: The row represented by a list, found based on the value.
        :rtype: pandas series
        :raises TypeError: If column is neither an int or int64.
        :raises TypeError: If value is not a float.
        :raises ValueError: If the column parameter is not inside of the tables width.
        :raised ValueError: If the value is not found in the given column
        """
        # Check if the column is valid.
        if not isinstance(column, (int, np.int64)):
                raise TypeError("column must be int or int64, got", type(column).__name__)
        if not 0 <= column < self.get_width():
                raise ValueError("column must be between 0 and", self.get_width()-1)
        # Check if the value is a float.
        if not isinstance(value, float):
                raise TypeError("value must be float, got", type(value).__name__)

        idx = self.df.index[self.get_column(column) == value]
        # Check if the value is in this table.
        if len(idx) == 0:
            raise ValueError("value:", value, "not found in the given column:", column)
        
        return self.get_row(idx[0])

    def get_width(self):
        """
        Getter method for the width of the table represented by this class instance.

        :return: The width of the table represented by this class instance.
        :rtype: int
        """
        return len(self.df.columns)
    
    def get_length(self):
        """
        Getter method for the length of the table represented by this class instance.
        
        :return: The length of the table represented by this class instance.
        :rtype: int
        """
        return len(self.df)
    
    def get_point(self, row: int,  column: int):
        """
        Getter method for the value of a specific point in the table represented by this class instance. 

        :param row: The row in which the point is held.
        :type row: int or int64
        :param column: Ihe column in which the point is held.
        :type column: int or int64
        :return: The value in the called cell.
        :rtype: The type of the cell value.
        :raises TypeError: If column is neither an int or int64.
        :raises ValueError: If the column parameter is not inside of the tables width.
        :raises TypeError: If row is neither an int or int64.
        :raises ValueError: If the row parameter is not inside of the tables length.
        """
        # Check if the column is valid.
        if not isinstance(column, (int, np.int64)):
                raise TypeError("column must be int or int64, got", type(column).__name__)
        if not 0 <= column < self.get_width():
                raise ValueError("column must be between 0 and", self.get_width()-1)
        # Check if the row is valid.
        if not isinstance(row, (int, np.int64)):
                raise TypeError("row must be int or int64, got", type(row).__name__)
        if not 0 <= row < self.get_length():
                raise ValueError("row must be between 0 and", self.get_length()-1)
        
        return self.df.iat[row, column]

    def save_to_sql(self):
        """
        Saves the table represented by this class instance in a local sql-file.
        """
        self.sql_save_count += 1 
        self.df.to_sql(name=self.table_name, con=self.engine, if_exists="replace", index=False)

    def read_from_sql(self):
        """
        Loads the table represented by this class instance from the local sql-file.
        It has to be saved once, bevor it can be loaded.

        :return: A list holding the loaded table from the sql-file.
        :rtype: pandas DataFrame
        :raises RuntimeError: If the method save_to_sql() was never called for this table before this method call.
        """
        # Check if this table was saved to sql at least once.
        if self.sql_save_count == 0:
                raise RuntimeError("read_from_sql() called before save_to_sql() was called once.")
        
        return pd.read_sql_table(table_name=self.table_name, con=self.engine, index_col=None)
    
    def close_sql_connection(self):
        """
        Closes the sql connection and clears the reference.
        Disposes the sql engine and clears the reference.
        """
        # Close and clear the sql connection.
        if self.connection is not None:
                self.connection.close()
                self.connection = None
        
        # Dispose and clear the sql engine.
        if self.engine is not None:
                self.engine.dispose()
                self.engine = None
    
class TableFunction(TableClass):
    """
    Child class of TableClass.
    Saves and works with a Table with mutliple columns and rows in a class table using a pandas Dataframe.
    Columns represent functions and rows the values of the functions.
    The first column saves the x_coordinates and the following columns the y_coordinates of the functions, 
    all in reference to the x_coords.
    Saves the highest deviation respective to each function in the table. 
    The highest deviation is calculated in reference to a function which the respective function is compared against.
    Adds methods for comparing of functions represented by instances of this class.
    """
     
    def __init__(self, table_name: str, file_path: WindowsPath=None, data: dict=None):
        super().__init__(table_name=table_name, file_path=file_path, data=data)
        # Build the datastructure for saving the highest deviation for all functions in this table with default -1.0.
        data = [-1.0]
        width = len(self.df.columns)
        while len(data) < width:
            data.append(-1.0)
        self.highest_deviation = pd.Series(data)

    def set_highest_deviation(self, function_index: int, deviation: float):
        """
        Sets the value of the class datastructure at the given index to the given deviation.
        The class datastructure saves the deviation in reference to the function at the same index in this table. 
        
        :param function_index: The index of the function in this table, that is compared to all functions in the given table.
        :type function_index: int or int64
        :param deviation: The deviation that should be saved in the class datastructure.
        :type deviation: float
        :raises TypeError: If function_index is not int or int64.
        :raises ValueError: If function_index is not at least one and lower than this tables width.
        :raises TypeError: If deviation is not float.
        """
        # Check if the function_index is valid.
        if not isinstance(function_index, (int, np.int64)):
                raise TypeError("function index must be int, got", type(function_index).__name__)
        if not 1 <= function_index < self.get_width():
                raise ValueError("function_index must be between 1 and", self.get_width())
        # Check if the deviation is a float.
        if not isinstance(deviation, float):
                raise TypeError("deviation must be float, got", type(deviation).__name__)

        self.highest_deviation[function_index] = deviation

    def get_highest_deviations(self):
        """
        Getter method for a copy of the list, which contains the highest deviation of the last function comparison
        for each function saved in this table.

        :return: A list containing the deviations at the same index as the respective function in this table.
        :rType: pandas Series
        """
        return self.highest_deviation.copy()

    def compare_functions(self, y_coords_1: pd.Series[float], y_coords_2: pd.Series[float]):
        """
        Compares two functions by their y-deviations squared.
        The functions must be given by their y coordinates with the same intervals between coordinates and the same length.

        :param y_coords_1: A list with the y coordinates for function 1.
        :type y_coords_1: A pandas Series containing floats.
        :param y_coords_2: A list with the y coordinates for function 2.
        :type y_coords_2: A pandas Series containing floats.
        :return: The sum of all y-deviations squared for all points in the lists.
        :rtype: float
        :raises TypeError: If y_coords_1 or y_coords_2 are not pandas series.
        :raises TypeError: If y_coords_1 or y_coords_2 do not contain only float values.
        :raises ValueError: If y_coords_1 and y_coords_2 are not of the same length.
        """
        # Check if y_coords_1 is valid.
        if not isinstance(y_coords_1, pd.Series):
                raise TypeError("y_coords_1 must be pandas Series, got", type(y_coords_1).__name__)
        if not pd.api.types.is_float_dtype(y_coords_1):
                raise TypeError("y_coords_1 must contain only floats")
        # Check if y_coords_2 is valid.
        if not isinstance(y_coords_2, pd.Series):
                raise TypeError("y_coords_2 must be pandas Series, got", type(y_coords_2).__name__)
        if not pd.api.types.is_float_dtype(y_coords_2):
                raise TypeError("y_coords_2 must contain only floats")

        sum = 0.0
        length_function_1 = len(y_coords_1)
        # Check if both given Series have the same length.
        if length_function_1 != len(y_coords_2):
                raise ValueError("The lists are not the same length. Got", length_function_1, "and", len(y_coords_2))
        # Calculate the sume of all deviations squared.
        for x in range(0, length_function_1):
            sum = sum + ((y_coords_1[x] - y_coords_2[x])**2)
        return sum
        
    def find_function_closest(self, function_index: int, table: TableFunction):
        """
        Compares the function represented in this table at the given function_index with all functions in the table.
        Finds the function out of the given table which has the smallest y-deviation
        squared sumed up over all points compared to the function in this table.
   
        :param function_index: The index of the function in this table, that is compared to all functions in the given table.
        :type function_index: int or int64
        :param table: A table with one or more functions, which are compared with the given function
        :type table: TableClass or child classes
        :return: Two-element list [y_coords_closest_name, y_coords_closest] where y_coords_closest_name is the name of the
          function and y_coords_closest is a list containing the coords of the closest function.
        :rtype: list [str, pandas.Series]
        :raises TypeError: If function_index is not int or int64.
        :raises ValueError: If function_index is not at least one and lower than this tables width.
        :raises TypeError: If table is not TableFunction.
        :raises ValueError: If this table and the given table do not have the same length.
        :raises ValueError: If this table and the given table do not have the same x-values.
        """
        # Check if function_index is valid.
        if not isinstance(function_index, (int, np.int64)):
                raise TypeError("function index must be int, got", type(function_index).__name__)
        if not 1 <= function_index < self.get_width():
                raise ValueError("function_index must be between 1 and", self.get_width())
        # Check if table is a TableFunction.
        if not isinstance(table, TableFunction):
                raise TypeError("table must be TableFunction, got", type(table).__name__)
        # Check if the given table has the same length as this table.
        if self.get_length() != table.get_length():
                raise ValueError("The lists are not the same length. Got", self.get_length(), "and", table.get_length())
        # Check if the two tables have the same x-values as basis for their functions.
        if not self.get_column(0).equals(table.get_column(0)):
                raise ValueError("The two functions do not have the x-values and therefore can't be compared.")

        # Set the variables for acces inside the loop
        y_coords_closest = []
        y_coords_closest_name = ""
        lowest_y_deviation = -1
        y_coords_compare = self.get_column(function_index)
        table_width = table.get_width()

        # Loop over all functions inside the given table. Skip index 0 because it holds the x_coordinates.
        for column in range(1, table_width):
            y_coords_loop = table.get_column(column)
            loop_deviation = self.compare_functions(y_coords_compare, y_coords_loop)

            # If its the first run through the loop, or the new calculated deviation is smaler than all the others before: 
            # Save deviation as the lowest so far and the respective function for a possible return.
            if lowest_y_deviation == -1 or (loop_deviation != -1 and lowest_y_deviation > loop_deviation):
                lowest_y_deviation = loop_deviation
                y_coords_closest = y_coords_loop
                y_coords_closest_name = table.get_column_names()[column]

        return [y_coords_closest_name, y_coords_closest]
    
    def find_highest_deviation(self, function_index: int, y_coords_2: pd.Series):
        """
        Finds the highest deviation between a function in this class table and a given function.
        The function in this class table is given by the respective index.
        Saves the deviation in the classvariable "highest_deviation" for the function in this class table and returns it.
        
        :param function_index: The index of the function in this class table, that is to be compared with the second function.
        :type function_index: int or int64
        :param y_coords_2: The y coordinates of a second function that is to be compared.
        :type y_coords_2: A pandas Series containing floats.
        :return: The calculated hightest deviation between the two functions.
        :rtype: float
        :raises TypeError: If function_index is not int or int64.
        :raises ValueError: If function_index is not at least one and lower than this tables width.
        :raises TypeError: If y_coords_2 is not pandas series.
        :raises TypeError: If y_coords_2 does not contain only float values.
        :raises ValueError: If y_coords_2 has not the same length as the table in this class.
        """
        if not isinstance(function_index, (int, np.int64)):
                raise TypeError("function index must be int or int64, got", type(function_index).__name__)
        if not 1 <= function_index < self.get_width():
                raise ValueError("function_index must be between 1 and", self.get_width())
        if not isinstance(y_coords_2, pd.Series or list):
                raise TypeError("df must be Panda Series or list, got", type(y_coords_2).__name__)
        if not pd.api.types.is_float_dtype(y_coords_2):
                raise TypeError("y_coords_2 must contain only floats")

        # Set the variables for acces inside the loop
        y_coords_1 = self.get_column(function_index)
        highest_deviation = 0.0
        length_function_1 = len(y_coords_1)

        # Check if the functions have the same length.
        if length_function_1 != len(y_coords_2):
                raise ValueError("The lists are not the same length. Got", length_function_1, "and", len(y_coords_2))
        
        # Loop over all coordinates to find the highest deviation.
        for x in range(0, length_function_1 - 1):
            loop_deviation = abs((y_coords_1[x] - y_coords_2[x]))
            # If the deviation found in this loop is higher than all of the others before: Save as new highest deviation.
            if loop_deviation > highest_deviation:
                highest_deviation = loop_deviation

        # Save the highest deviation for the function in the class datastructure and return it to the caller.
        self.set_highest_deviation(function_index= function_index, deviation=highest_deviation)
        return highest_deviation

class TablePoints(TableClass):
    """
    Child class of TableClass.
    Saves and works with a Table with mutliple columns and rows in a class table using a pandas Dataframe.
    Columns represent coordinates and rows the values of the coordinates.
    The first column saves the x_coordinates and the following column the y_coordinates. 
    The x and y coordinates together can be read as coordinates in a 2 dimensional system.
    Adds methods for comparing the points represented by instances of this class with .
    """
    
    def __init__(self, table_name: str, file_path: WindowsPath=None, data: dict=None):
        super().__init__(table_name=table_name, file_path=file_path, data=data)

    def compare_points(self, table: TableFunction):
        """
        Compares all the points represented by this class instance with the functions in the given table.
        Tests if the deviation of each comparison is inside the square root of the highest allowed deviation, 
        which is saved in the given table.
        If the test is positive this class instance saves the calculated deviation in column 3 of this class table
        and the function which it is close to in column 4 of this class table.  
        
        :param table: A table containing at least one function and the resprective highest deviation.
        :type table: TableFunction
        :raises TypeError: If table is not a TableFunction.
        :raises ValueError: If at least one of the functions in the given table has not highest deviation saved.
        """
        # Check if table is a TableFunction.
        if not isinstance(table, TableFunction):
                raise TypeError("table must be TableFunction, got", type(table).__name__)
        
        # Set the variables for acces inside the loop.
        points_length = self.get_length() 
        table_deviations = table.get_highest_deviations()
        table_width = table.get_width()
        column_deviation = {}
        column_mapping = {}

        # Check if the highest deviation is set for all functions in the table.
        for x in range(1, len(table_deviations)):
            if table_deviations.iloc[x] == -1.0:
                    raise ValueError("highest_deviation is not set for at least the function at index:", x)

        # Loop over all points represented by this class instance.
        for x in range(0, points_length):
            outer_loop_x = self.get_point(x, 0)
            outer_loop_y = self.get_point(x, 1)
            outer_loop_table_row = table.find_row(0, outer_loop_x)

            # Loop over all functions in the given table and compare the point from the outer loop against each one.
            for y in range(1, table_width):
                inner_loop_deviation = abs(outer_loop_table_row.iloc[y] - outer_loop_y)
                # If the claculated deviation between the point and the function is inside the square root of the maximum deviation:
                # Save the calculated deviation and the respective function in the variables. 
                if inner_loop_deviation <= math.sqrt(table_deviations[y]):
                    column_deviation[x] = inner_loop_deviation
                    column_mapping[x] = table.get_column_names()[y]
        
        # Save the calculated deviations for all points in this class table.
        self.df["delta Y"] = pd.Series(column_deviation)
        self.df["No. of ideal func"] = pd.Series(column_mapping)

    
