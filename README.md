File Combiner & Merger

Description

This is a desktop application with a graphical user interface (GUI) designed to automate the process of combining data from multiple files (.xlsx, .xlsm, .csv, .txt). The application also includes a powerful feature to merge the combined data with an external lookup file, enriching it with additional information.

This tool is ideal for data processing tasks that require consolidating information from various sources into a single, clean report.

Features
Combine Multiple File Types: Automatically finds and processes .xlsx, .xlsm, .csv, and .txt files from a specified folder.

Flexible Data Extraction: Allows you to specify the header row and data start row for your source files.

Selective Column Extraction: You can specify exactly which columns to extract from the source files.

Custom Delimiters: Supports custom delimiters for .txt files (e.g., tab \t, semicolon ;, etc.).

Data Merging: Merges the combined data with an external "lookup" file (Excel, CSV, or TXT) based on a common key column (similar to VLOOKUP in Excel).

Flexible Merge Options: Full control over the lookup file's structure, including specifying its header row and the columns you want to add.

Choice of Output Format: Save the final report as either a .csv or .xlsx file.

User-Friendly Interface: All settings are managed through a simple graphical interface, and progress is displayed in a log window.

Persistent Settings: All your configurations are saved locally in a config.json file, so you don't have to re-enter them every time.

Requirements
Python 3.7+

The following Python libraries:

pandas

openpyxl

pyinstaller (only for building the executable)

Installation
Clone or download the repository.

Install the required libraries by opening a terminal in the project folder and running:

pip install pandas openpyxl

How to Use
Run the application:

python combine_app_en.py

Configure Settings:

Click the "Settings" button to open the configuration window.

Input Settings:

Select the folder containing your source files.

Specify the sheet name (for Excel files).

If you are processing .txt files, check the "Process .txt files" box and enter the correct delimiter (\t for tab).

Set the header and data start row numbers.

List the columns you want to extract, separated by commas.

Merge Settings (Optional):

Check "Enable Merge" to activate this feature.

Select your lookup file (e.g., a master price list).

Specify the header and data start rows for the lookup file.

If the lookup file is a .txt, enable its checkbox and set its delimiter.

Enter the key column names for both the source and lookup files (e.g., SKU).

List the columns you want to add from the lookup file.

Output Settings:

Choose a folder to save the final report.

Enter a filename (without extension).

Select the output format (csv or xlsx).

Click "Save Settings".

Start Processing:

Click the "Start" button on the main window.

The application will begin processing the files, and you can monitor the progress in the log window.

When finished, a confirmation message will appear, and your report will be saved in the specified output folder.

How to Build the Executable (.exe)
You can package this application into a single .exe file for Windows so it can be run without needing Python installed.

Install PyInstaller:

pip install pyinstaller

Run the build command from the terminal in your project directory:

pyinstaller --onefile --windowed --name="FileCombiner" combine_app_en.py

The final executable file, FileCombiner.exe, will be located in the dist folder.
