# Earthquake Data Analysis with PySpark

This PySpark application analyzes earthquake data, performs various transformations and calculations, and visualizes the earthquakes on a map using Folium.

## Instructions:

### Prerequisites:
- Python 3.x installed
- Apache Spark installed and configured (locally or on a cluster)
- Python packages installed (requirements file enclosed)

### Steps to Run the Application:

1. Clone the repository or download the source code.

2. Make sure you have the `database.csv` file containing the earthquake data in the same directory as the script.

3. Open a terminal or command prompt.

4. Navigate to the directory containing the source code.

5. Run the following command to execute the PySpark application:
   ```
   python3 earthquake_analysis.py
   ```

6. The application will process the earthquake data, perform transformations and calculations, generate a Folium map showing the earthquake locations, and save it as `earthquake_map.html`.

7. The transformed data will be saved as CSV files in the `transformed_df` directory.

8. You can view the output and analysis results in the terminal.

## Additional Notes:

- Ensure that all necessary dependencies are installed. You can install the required Python libraries using `pip install -r requirements.txt`.

- Adjust the file paths and configurations in the script (`earthquake_analysis.py`) as per your environment if necessary.
