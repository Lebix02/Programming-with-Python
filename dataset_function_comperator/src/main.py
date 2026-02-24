from .tables import TablePoints, TableFunction 
from .visualizer import TableVisualization
from pathlib import Path

MODULE_DIR = Path(__file__).resolve().parent
DATA_DIR = MODULE_DIR / "data"

TEST_DATA = DATA_DIR / "test.csv"
IDEAL_DATA = DATA_DIR / "ideal.csv"
TRAINING_DATA = DATA_DIR / "train.csv"

def main():
   # 1) Create the Tables for the raw datasets, visualize them, and save them to sql.
   table_ideal = TableFunction(table_name="ideal_functions", file_path=IDEAL_DATA)
   visualization_ideal = TableVisualization(file_name="ideal_function", visualization_title="Ideal functions")
   visualization_ideal.add_functions(table_ideal)
   visualization_ideal.show()
   table_ideal.save_to_sql()

   table_train = TableFunction(table_name="train_functions", file_path=TRAINING_DATA)
   visualization_train = TableVisualization(file_name="training_functions", visualization_title="Training functions")
   visualization_train.add_functions(table_train)
   visualization_train.show()
   table_train.save_to_sql()

   table_test = TablePoints(table_name="test_data", file_path=TEST_DATA)
   visualization_test = TableVisualization(file_name="test_points", visualization_title="Test points")
   visualization_test.add_points(table_test)
   visualization_test.show()

   # 2) Compute the solution of the comparison
   solution_comparison = {"x": table_train.get_column(0)}
   table_train_width = table_train.get_width()
   for x in range(1, table_train_width):
      loop_solution = table_train.find_function_closest(x, table_ideal)
      solution_comparison[loop_solution[0]] = loop_solution[1]

   # 3) Create the table that represents the solution and compute the lowest deviations for each column.
   #    Display the mapping between the ideal functions and the training functions. 
   #    The Visualization leaves out function 2 of the train data and y24 of the ideal data because their
   #    scaling does not allow effective visualization of the difference between training and ideal data.
   table_ideal_compared = TableFunction(table_name="compared functions", data=solution_comparison)
   
   visualization_ideal_compared = TableVisualization(file_name="functions_mapped", visualization_title="Training functions mapped")
   visualization_ideal_compared.add_functions(table_ideal_compared, {1,3,4})
   visualization_ideal_compared.add_functions(table_train, {1,3,4})
   visualization_ideal_compared.show()
   table_ideal_compared.save_to_sql()

   table_ideal_compared_width = table_ideal_compared.get_width()
   for x in range(1, table_ideal_compared_width):
      loop_deviation = table_train.find_highest_deviation(x, table_ideal_compared.get_column(x))
      table_ideal_compared.set_highest_deviation(x, loop_deviation)

   table_test.save_to_sql()

   # 4) Compute the points, that can be mapped to the four ideal functions.
   #    Visualizes the calculated four ideal functions out of all ideal functions 
   #    and the points that are inside the calculated range of these functions.
   #    The Visualization is split in two because of the scale of function "y24".
   #    Save the resulting table with points and their mapping to sql.
   table_test.compare_points(table_ideal_compared)

   visualization_result = TableVisualization(file_name="points_mapped_1", visualization_title="Test points mapped 1")
   visualization_result.add_functions(table_ideal_compared, {1,3,4})
   visualization_result.add_points(table_test, {"y13", "y36", "y40"})
   visualization_result.show()
   visualization_result_2 = TableVisualization(file_name="points_mapped_2", visualization_title="Test points mapped 2")
   visualization_result_2.add_functions(table_ideal_compared, {2})
   visualization_result_2.add_points(table_test, {"y24"})
   visualization_result_2.show() 
   table_test.save_to_sql()

   print("All plots have been created and displayed successfully.")

   # 6) Close all sql connections.
   table_ideal.close_sql_connection()
   table_train.close_sql_connection()
   table_ideal_compared.close_sql_connection()
   table_test.close_sql_connection()

   print("All SQL connections have been closed and the program execution is finished successfully.")

   

if __name__ == '__main__':
   main()