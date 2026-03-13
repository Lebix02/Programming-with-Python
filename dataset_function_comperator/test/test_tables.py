import unittest
import pandas as pd
from src.tables import TableClass, TableFunction, TablePoints
from pathlib import Path

MODULE_DIR = Path(__file__).resolve().parent
TEST_DATA_DIR = MODULE_DIR / "data"

TEST_FUNCTIONS_1 = TEST_DATA_DIR / "unit_test_functions_1.csv"
TEST_FUNCTIONS_2 = TEST_DATA_DIR / "unit_test_functions_2.csv"
TEST_POINTS = TEST_DATA_DIR / "unit_test_points.csv"

# Tests for the class TableClass.
class TestTableClass(unittest.TestCase):
    def setUp(self):
        # Set up a test class.
        self.tc = TableClass(table_name="test_tc", file_path=TEST_POINTS)

    def tearDown(self):
        # Close connection to avoid ResourceWarning from sqlite3.
        self.tc.close_sql_connection()

# Tests for the constructor.
    def test_constructor_happy_path_file_name(self):
        expected_df = pd.DataFrame({"x": [-10.0, -9.0, -8.0, -1.0, -10.0],
                                    "y1": [-10.0, 9.5, -7.5, 1000.0, -102.0]})
        
        # This test inspects the internal DataFrame to ensure that the consturctor reads the data correctly.
        # Even though the direct access to the wrappers data is not part of the public API it is required here.
        pd.testing.assert_frame_equal(self.tc._get_full_df_for_tests(), expected_df)

    
    def test_constructor_happy_path_dictonary(self):
        dictonary = {"x": [-10.0, -9.0, -8.0, -1.0, -10.0],
                    "y1": [-10.0, 9.5, -7.5, 1000.0, -102.0]}
        
        test_table_class = TableClass(table_name="test_name", data=dictonary)
        expected_df = pd.DataFrame(dictonary)

        try:
            # This test inspects the internal DataFrame to ensure that the consturctor reads the data correctly.
            # Even though the direct access to the wrappers data is not part of the public API it is required here.
            pd.testing.assert_frame_equal(test_table_class._get_full_df_for_tests(), expected_df)
        finally:
            test_table_class.close_sql_connection()

    def test_constructor_file_not_found(self):
        # The constructor cateches the FileNotFoundError and raises a ValueError with the message of an invalid file name.
        bad_file_path = TEST_DATA_DIR / "bad_file_name.csv"
        with self.assertRaises(ValueError):
            TableClass(table_name="test_name", file_path=bad_file_path)

    def test_constructor_non_string_file_name(self):
        # File_name is a float not a string.
        with self.assertRaises(TypeError):
            TableClass(table_name="test_name", file_path=1.0)

    def test_constructor_non_string_table_name(self):
        # Table_name is a float not a string.
        with self.assertRaises(TypeError):
            TableClass(table_name=1.0, file_path=TEST_POINTS)

    def test_constructor_empty_string_table_name(self):
        # Table_name is the empty string which is not valid.
        with self.assertRaises(ValueError):
            TableClass(table_name="", file_path=TEST_POINTS)      

    def test_constructor_whitespaces_string_table_name(self):
        # Table_name consists of only whitespaces which is not valid.
        with self.assertRaises(ValueError):
            TableClass(table_name="   ", file_path=TEST_POINTS)

    def test_constructor_non_dict_data(self):
        # Data is a string not a dictonary.
        with self.assertRaises(TypeError):
            TableClass(table_name="test_name", data=TEST_POINTS)

    def test_constructor_no_file_name_and_no_data(self):
        # The constructor must be called with a valid file_name or a valid dictonary. As one of them can be none 
        # it is technically possible to call the constructor without both of them. The expected behavior is a raised
        # ValueError.
        with self.assertRaises(ValueError):
            TableClass(table_name="test_name")   

    def test_constructor_data_not_floats(self):
        test_dict = {"x": [-10.0, -9.0, -8.0, -1.0, "string"],
                                    "y1": [-10.0, 9.5, -7.5, 1000.0, -102.0]}
        with self.assertRaises(TypeError):
                TableClass(table_name="test_name", data=test_dict)
        
    def test_constructor_missing_column_two(self):
        test_dict = {"x": [-10.0, -9.0, -8.0, -1.0, -10.0]}
        with self.assertRaises(ValueError):
                TableClass(table_name="test_name", data=test_dict)  

# Tests for saving and reading SQL.
    def test_save_and_read_sql_happy_path(self):
        expected_df = pd.DataFrame({"x": [-10.0, -9.0, -8.0, -1.0, -10.0],
                                    "y1": [-10.0, 9.5, -7.5, 1000.0, -102.0]})
        self.tc.save_to_sql()
        result_df = self.tc.read_from_sql()
        pd.testing.assert_frame_equal(expected_df, result_df)

    def test_read_sql_before_saved(self):
        # The dataframe cant be read from sql if it never was saved before. The method raises a RuntimeError in that case.
        with self.assertRaises(RuntimeError):
            self.tc.read_from_sql()
    

# Tests for the class TableFunction.
class TestTableFunction(unittest.TestCase):
    def setUp(self):
        # Set up the test classes.
        self.tf_1 = TableFunction(table_name="test_tf_1", file_path=TEST_FUNCTIONS_1)
        self.tf_2 = TableFunction(table_name="test_tf_2", file_path=TEST_FUNCTIONS_2)

    def tearDown(self):
        # Close connections to avoid ResourceWarning from sqlite3.
        for obj in (self.tf_1, self.tf_2):
            obj.close_sql_connection()

# Tests for the method compare_functions(y_coords_1, y_coords_2) in TableFunction.
    def test_compare_functions_happy_path(self):
        # y1 = [1.0, 2.0, 3.0]
        # y2 = [1.0, 4.0, 9.0]
        # deviations^2: (0)^2 + (-2)^2 + (-6)^2 = 0 + 4 + 36 = 40
        y1 = pd.Series([1.0, 2.0, 3.0], dtype="float64")
        y2 = pd.Series([1.0, 4.0, 9.0], dtype="float64")

        result = self.tf_1._compare_functions(y_coords_1=y1, y_coords_2=y2)
        self.assertAlmostEqual(result, 40.0)

    def test_compare_functions_non_series_first_param(self):
        y1 = [1.0, 2.0, 3.0]   # y1 is a List, not a pandas Series.
        y2 = pd.Series([1.0, 2.0, 3.0], dtype="float64")

        with self.assertRaises(TypeError):
            self.tf_1._compare_functions(y_coords_1=y1, y_coords_2=y2)

    def test_compare_functions_non_series_second_param(self):
        y1 = pd.Series([1.0, 2.0, 3.0], dtype="float64")
        y2 = [1.0, 2.0, 3.0]   # y2 is a List, not a pandas Series.

        with self.assertRaises(TypeError):
            self.tf_1._compare_functions(y_coords_1=y1, y_coords_2=y2)

    def test_compare_functions_non_float_dtype_first_param(self):
        # y1 is a Series of int not float dtype.
        y1 = pd.Series([1, 2, 3], dtype="int64")
        y2 = pd.Series([1.0, 2.0, 3.0], dtype="float64")

        with self.assertRaises(TypeError):
            self.tf_1._compare_functions(y_coords_1=y1, y_coords_2=y2)

    def test_compare_functions_non_float_dtype_second_param(self):
        y1 = pd.Series([1.0, 2.0, 3.0], dtype="float64")
        # y2 is a Series of int not float dtype.
        y2 = pd.Series([1, 2, 3], dtype="int64") 

        with self.assertRaises(TypeError):
            self.tf_1._compare_functions(y_coords_1=y1, y_coords_2=y2)

    def test_compare_functions_unequal_length(self):
        # y1 and y2 have different lengths, therefore the comparison is not possible.
        y1 = pd.Series([1.0, 2.0, 3.0], dtype="float64")
        y2 = pd.Series([1.0, 2.0], dtype="float64")

        with self.assertRaises(ValueError):
            self.tf_1._compare_functions(y_coords_1=y1, y_coords_2=y2)

# Tests for the method find_function_closest(function_index, table) in class TableFunction.   
    def test_find_function_closest_happy_path(self):
        # The first column in tf_1:
        # [-10.0, -9.0, -8.0, ..., 8.0, 9.0, 10.0]
        # The fifth column in tf_2:
        # [-11.0, -10.0, -9.0, ..., 7.0, 8.0, 9.0]
        # There are no other functions in tf_2 that are as close to function 1 in tf_1.
        result = self.tf_1.find_function_closest(function_index=1, table=self.tf_2)
        name, function = result
        
        self.assertEqual(name, "y4")
        pd.testing.assert_series_equal(function, self.tf_2.get_column(4))
    
    def test_find_function_closest_non_int_type_first_param(self):
        with self.assertRaises(TypeError):
            # Param function_index is a float type not int.
            self.tf_1.find_function_closest(function_index=1.5, table=self.tf_2)

    def test_find_function_closest_outside_range_first_param(self):
        with self.assertRaises(ValueError):
            # Param function_index is out of range 0 to 4.
            self.tf_1.find_function_closest(function_index=6, table=self.tf_2)

    def test_find_function_closest_non_TableFunction_type_second_param(self):
        with self.assertRaises(TypeError):
            # Param table is a list not TableFunction.
            self.tf_1.find_function_closest(function_index=1, table=[1.0, 2.0, 3.0])

    def test_find_function_closest_unequal_length(self):
        test_tf = TableFunction(
            table_name="test_tf",
            data={"x": [1.0, 2.0, 3.0], "y1": [1.0, 2.0, 3.0]}
        )
        try:
            with self.assertRaises(ValueError):
                # Param test_tf too short.
                self.tf_1.find_function_closest(function_index=1, table=test_tf)
        finally:
            test_tf.close_sql_connection()     

    def test_find_functions_closest_different_x_value_functions(self):
        dictonary = {"x": [-10.0, -9.0, -8.0, -1.0, -10.0],
                    "y1": [-10.0, 9.5, -7.5, 1000.0, -102.0]}
        
        test_table_function = TableFunction(table_name="test_name", data=dictonary)

        try:
            with self.assertRaises(ValueError):
                # Different x-value functions can't be compared.
                self.tf_1.find_function_closest(function_index=1, table=test_table_function)
        finally:
            test_table_function.close_sql_connection()

# Tests for the method find_highest_deviation(function_index, y_coords_2).
    def test_find_highest_deviation_function_index_type_error(self):
        with self.assertRaises(TypeError):
            # Param function_index is a string not int.
            self.tf_2.find_highest_deviation(function_index="1", y_coords_2=self.tf_1.get_column(1))

    def test_find_highest_deviation_function_index_range_error(self):
        with self.assertRaises(ValueError):
            # Param function_index is out of range 0 to 4.
            self.tf_2.find_highest_deviation(function_index=5, y_coords_2=self.tf_1.get_column(1))

    def test_find_highest_deviation_y_coords_2_type_error_list(self):
        with self.assertRaises(TypeError):
            # Param y_coords_2 is a TableFunction not a series.
            self.tf_2.find_highest_deviation(function_index=1, y_coords_2=self.tf_1)

    def test_find_highest_deviation_y_coords_2_type_error_content(self):
        with self.assertRaises(TypeError):
            # Param y_coords_2 is a series of mixed types not a series of only floats.
            self.tf_2.find_highest_deviation(function_index=1, y_coords_2=pd.Series(
                [-10.0, "-9.0", 1, [1.0], 3.0]))

    def test_find_highest_deviation_y_coords_2_value_error_len(self):
        with self.assertRaises(ValueError):
            # Param y_coords_2 is shorter than then tf_2.
            self.tf_2.find_highest_deviation(function_index=1, y_coords_2=pd.Series([1.0, 2.0, 3.0]))

    def test_find_highest_deviation_happy_path(self):

        result = self.tf_1.find_highest_deviation(function_index=1, y_coords_2=self.tf_2.get_column(4))

        # The highest deviation between the two functions is 1.0.
        self.assertAlmostEqual(result, 1.0)
        self.assertAlmostEqual(self.tf_1.get_highest_deviations()[1], 1.0)


# Tests for the Class TablePoints.
class TestTablePoints(unittest.TestCase):
    def setUp(self):
        # Set up the test classes.
        self.tp_1 = TablePoints(table_name="test_tp_1", file_path=TEST_POINTS)
        self.tp_2 = TablePoints(table_name="test_tp_1", file_path=TEST_POINTS)
        self.tf = TableFunction(table_name="test_tf_1", file_path=TEST_FUNCTIONS_1)

    def tearDown(self):
        # Close connections to avoid ResourceWarning from sqlite3.
        for obj in (self.tf, self.tp_1, self.tp_2):
            obj.close_sql_connection()

# Tests for the method test_points(table).
    def test_test_points_table_non_TableFunction(self):
        with self.assertRaises(TypeError):
            # Param table is a TablePoints instance not a TableFunction instance.
            self.tp_1.compare_points(table=self.tp_2)

    def test_test_points_table_without_deviation_first_function(self):
        # Set the deviations for the table.
        self.tf.set_highest_deviation(function_index=2, deviation=10.0)
        self.tf.set_highest_deviation(function_index=3, deviation=5.0)
        self.tf.set_highest_deviation(function_index=4, deviation=0.0)
        
        with self.assertRaises(ValueError):
            # Devitation of function y1 in table tf_2 is not set. 
            self.tp_1.compare_points(table=self.tf)

    def test_test_points_table_without_deviation_middle_function(self):
        # Set the deviations for the table.
        self.tf.set_highest_deviation(function_index=1, deviation=10.0)
        self.tf.set_highest_deviation(function_index=3, deviation=5.0)
        self.tf.set_highest_deviation(function_index=4, deviation=0.0)
        
        with self.assertRaises(ValueError):
            # Devitation of function y2 in table tf is not set.
            self.tp_1.compare_points(table=self.tf)

    def test_test_points_table_without_deviation_last_function(self):
        # Set the deviations for the table.
        self.tf.set_highest_deviation(function_index=1, deviation=10.0)
        self.tf.set_highest_deviation(function_index=2, deviation=5.0)
        self.tf.set_highest_deviation(function_index=3, deviation=0.0)
        
        with self.assertRaises(ValueError):
            # Devitation of function y4 in table tf_2 is not set.
            self.tp_1.compare_points(table=self.tf)

    def test_test_points_happy_path(self):
        # Set all the deviations for the table.
        self.tf.set_highest_deviation(function_index=1, deviation=20.0)
        self.tf.set_highest_deviation(function_index=2, deviation=10.0)
        self.tf.set_highest_deviation(function_index=3, deviation=5.0)
        self.tf.set_highest_deviation(function_index=4, deviation=0.0)

        result_delta = pd.Series(
            data={0: 0.0, 1: 0.5, 2:0.5, 3: float('nan'), 4: 2.0},
            name="delta Y"
        ) 
        result_no = pd.Series(
            data={0: "y1", 1: "y2", 2: "y1", 3: float('nan'), 4: "y3"},
            name="No. of ideal func"
        ) 

        self.tp_1.compare_points(table=self.tf)

        pd.testing.assert_series_equal(result_delta, self.tp_1.get_column(2))
        pd.testing.assert_series_equal(result_no, self.tp_1.get_column(3))

