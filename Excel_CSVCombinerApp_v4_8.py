import pandas as pd
import os
import glob
import json
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext

# --- CONFIGURATION ---
CONFIG_FILE = 'config.json'

# --- Settings Management ---

def load_settings():
    """Loads settings from a JSON file. Returns default values if the file doesn't exist."""
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        # Default values
        return {
            "input_folder": "",
            "output_folder": "",
            "sheet_name": "Sheet1",
            "header_row": 5,
            "data_start_row": 7,
            "columns_to_extract": "SKU, Price, Quantity",
            "output_filename": "combined_data",
            "output_format": "csv",
            "enable_merge": False,
            "lookup_file_path": "",
            "source_key_column": "SKU",
            "lookup_key_column": "SKU",
            "lookup_columns_to_add": "ASIN, Description",
            "enable_txt_processing": False,
            "txt_delimiter": "\\t",
            "enable_lookup_txt": False,
            "lookup_txt_delimiter": "\\t",
            "lookup_header_row": 1,
            "lookup_data_start_row": 2
        }

def save_settings(settings):
    """Saves settings to a JSON file."""
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(settings, f, indent=4)

# Helper function to make column names unique
def _make_columns_unique(columns):
    """Takes a list of column names and makes them unique by adding suffixes."""
    seen = {}
    new_columns = []
    for col in columns:
        col_str = str(col) # Ensure column name is a string
        if col_str in seen:
            seen[col_str] += 1
            new_columns.append(f"{col_str}.{seen[col_str]}")
        else:
            seen[col_str] = 0
            new_columns.append(col_str)
    return new_columns

# --- Core File Processing Logic ---

def process_files(settings, log_callback):
    """
    Core logic to process files, with robust encoding and duplicate column handling.
    """
    input_folder = settings['input_folder']
    header_row_index = settings['header_row'] - 1
    data_start_row_index = settings['data_start_row'] - 1
    columns_to_extract = [col.strip() for col in settings['columns_to_extract'].split(',') if col.strip()]
    input_txt_delimiter = settings.get('txt_delimiter', '\t').encode().decode('unicode_escape')

    log_callback(f"--- Starting to process files in folder: {input_folder} ---\n")
    
    file_patterns = ["*.xlsx", "*.xlsm", "*.csv"]
    if settings.get('enable_txt_processing'):
        file_patterns.append("*.txt")
        log_callback("Input TXT file processing is enabled.\n")

    all_files = []
    for pattern in file_patterns:
        all_files.extend(glob.glob(os.path.join(input_folder, pattern)))

    if not all_files:
        log_callback(f"ERROR: No files matching the specified types found in the folder.\n")
        return

    log_callback(f"Found {len(all_files)} files to process.\n")
    all_data_frames = []

    for i, file_path in enumerate(all_files, 1):
        log_callback(f"\n[{i}/{len(all_files)}] -> Processing file: {os.path.basename(file_path)}\n")
        try:
            df_full = None
            if file_path.endswith(('.xlsx', '.xlsm')):
                df_full = pd.read_excel(file_path, sheet_name=settings['sheet_name'], header=None)
            else: 
                try:
                    if file_path.endswith('.csv'):
                        df_full = pd.read_csv(file_path, header=None, on_bad_lines='skip', encoding='utf-8')
                    elif file_path.endswith('.txt'):
                        df_full = pd.read_csv(file_path, header=None, on_bad_lines='skip', encoding='utf-8', sep=input_txt_delimiter, engine='python')
                except UnicodeDecodeError:
                    log_callback(f"  - WARNING: UTF-8 decoding failed for {os.path.basename(file_path)}. Trying with latin-1 encoding.\n")
                    if file_path.endswith('.csv'):
                        df_full = pd.read_csv(file_path, header=None, on_bad_lines='skip', encoding='latin-1')
                    elif file_path.endswith('.txt'):
                        df_full = pd.read_csv(file_path, header=None, on_bad_lines='skip', encoding='latin-1', sep=input_txt_delimiter, engine='python')

            if df_full is None or len(df_full) < data_start_row_index:
                log_callback(f"  - WARNING: Not enough rows in the file or failed to read. Skipping.\n")
                continue

            column_names = df_full.iloc[header_row_index]
            unique_column_names = _make_columns_unique(column_names)
            data_df = df_full.iloc[data_start_row_index:].copy()
            data_df.columns = unique_column_names
            data_df.reset_index(drop=True, inplace=True)
            
            if columns_to_extract:
                data_df.columns = data_df.columns.astype(str)
                existing_cols = [col for col in columns_to_extract if col in data_df.columns]
                missing_cols = [col for col in columns_to_extract if col not in data_df.columns]
                
                if missing_cols:
                    log_callback(f"  - WARNING: Missing columns: {', '.join(missing_cols)}\n")
                if not existing_cols:
                    log_callback("  - WARNING: None of the specified columns were found. Skipping file.\n")
                    continue
                data_df = data_df[existing_cols]

            data_df['source_file'] = os.path.basename(file_path)
            all_data_frames.append(data_df)
            log_callback(f"  - Successfully extracted {len(data_df)} rows.\n")

        except Exception as e:
            log_callback(f"  - ERROR while processing file: {e}\n")

    if not all_data_frames:
        log_callback("\nCould not extract data from any file.\n")
        return

    log_callback("\n--- Combining all data... ---\n")
    final_df = pd.concat(all_data_frames, ignore_index=True)
    log_callback(f"Combined data has {len(final_df)} rows.\n")

    # --- Merge Logic ---
    if settings.get('enable_merge'):
        log_callback("\n--- Starting merge process... ---\n")
        try:
            lookup_file = settings['lookup_file_path']
            source_key = settings['source_key_column']
            lookup_key = settings['lookup_key_column']
            cols_to_add = [col.strip() for col in settings['lookup_columns_to_add'].split(',') if col.strip()]
            lookup_header_row_index = settings['lookup_header_row'] - 1
            lookup_data_start_row_index = settings['lookup_data_start_row'] - 1

            log_callback(f"Reading lookup file: {lookup_file}\n")
            lookup_df_full = None
            try:
                if settings.get('enable_lookup_txt') and lookup_file.endswith('.txt'):
                    lookup_txt_delimiter = settings.get('lookup_txt_delimiter', '\t').encode().decode('unicode_escape')
                    lookup_df_full = pd.read_csv(lookup_file, sep=lookup_txt_delimiter, engine='python', encoding='utf-8', header=None)
                elif lookup_file.endswith('.csv'):
                    lookup_df_full = pd.read_csv(lookup_file, encoding='utf-8', header=None)
                else:
                    lookup_df_full = pd.read_excel(lookup_file, header=None)
            except UnicodeDecodeError:
                log_callback("  - WARNING: UTF-8 decoding failed for lookup file. Trying with latin-1 encoding.\n")
                if settings.get('enable_lookup_txt') and lookup_file.endswith('.txt'):
                    lookup_txt_delimiter = settings.get('lookup_txt_delimiter', '\t').encode().decode('unicode_escape')
                    lookup_df_full = pd.read_csv(lookup_file, sep=lookup_txt_delimiter, engine='python', encoding='latin-1', header=None)
                elif lookup_file.endswith('.csv'):
                    lookup_df_full = pd.read_csv(lookup_file, encoding='latin-1', header=None)
            
            lookup_column_names = lookup_df_full.iloc[lookup_header_row_index]
            unique_lookup_column_names = _make_columns_unique(lookup_column_names)
            lookup_df = lookup_df_full.iloc[lookup_data_start_row_index:].copy()
            lookup_df.columns = unique_lookup_column_names
            lookup_df.reset_index(drop=True, inplace=True)

            final_df[source_key] = final_df[source_key].astype(str)
            lookup_df[lookup_key] = lookup_df[lookup_key].astype(str)

            # NEW: Smarter merge logic
            # Ensure the lookup key is in the subset, and remove duplicates from cols_to_add
            columns_for_subset = [lookup_key] + [col for col in cols_to_add if col != lookup_key]
            lookup_subset = lookup_df[columns_for_subset]

            final_df = pd.merge(final_df, lookup_subset, left_on=source_key, right_on=lookup_key, how='left')
            
            # Drop the lookup key ONLY if it has a different name than the source key
            # AND it was NOT explicitly requested in the columns to add.
            if source_key != lookup_key and lookup_key not in cols_to_add:
                final_df = final_df.drop(columns=[lookup_key])
            
            log_callback("Merge successful.\n")

        except Exception as e:
            log_callback(f"  - ERROR during merge process: {e}\n")
            messagebox.showerror("Merge Error", f"An error occurred during the merge process:\n{e}")

    # --- Save Logic ---
    output_folder = settings['output_folder']
    output_filename = settings['output_filename']
    output_format = settings['output_format']
    full_output_path = os.path.join(output_folder, f"{output_filename}.{output_format}")

    try:
        if output_format == 'xlsx':
            final_df.to_excel(full_output_path, index=False)
        else: # Default to CSV
            final_df.to_csv(full_output_path, index=False, encoding='utf-8-sig')
        
        log_callback(f"\n🎉 Done! All data has been combined into file: {full_output_path}\n")
        log_callback(f"Total rows processed: {len(final_df)}\n")
        messagebox.showinfo("Done", f"Processing complete! Result saved in {full_output_path}")
    except Exception as e:
        log_callback(f"  - ERROR saving the final report: {e}\n")
        messagebox.showerror("Save Error", f"Could not save the report file:\n{e}")


# --- Graphical User Interface (GUI) ---

class SettingsWindow(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        
        self.transient(parent)
        self.grab_set()

        self.title("Settings")
        self.geometry("732x650")
        self.parent = parent
        self.settings = load_settings()

        self.entries = {}
        self.merge_enabled_var = tk.BooleanVar(value=self.settings.get('enable_merge', False))
        self.txt_enabled_var = tk.BooleanVar(value=self.settings.get('enable_txt_processing', False))
        self.lookup_txt_enabled_var = tk.BooleanVar(value=self.settings.get('enable_lookup_txt', False))

        main_frame = ttk.Frame(self)
        main_frame.pack(fill="both", expand=True)
        canvas = tk.Canvas(main_frame)
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar = ttk.Scrollbar(main_frame, orient="vertical", command=canvas.yview)
        scrollbar.pack(side="right", fill="y")
        canvas.configure(yscrollcommand=scrollbar.set)
        self.scrollable_frame = ttk.Frame(canvas)
        canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        self.scrollable_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))

        # --- Input Settings ---
        input_frame = ttk.LabelFrame(self.scrollable_frame, text="Input Settings")
        input_frame.pack(padx=10, pady=10, fill="x")
        
        ttk.Label(input_frame, text="Folder with source files:").grid(row=0, column=0, padx=10, pady=5, sticky="w")
        self.entries['input_folder'] = ttk.Entry(input_frame, width=60)
        self.entries['input_folder'].grid(row=0, column=1, padx=10, pady=5, sticky="ew")
        ttk.Button(input_frame, text="Browse...", command=self.browse_input_folder).grid(row=0, column=2, padx=5, pady=5)
        
        ttk.Label(input_frame, text="Sheet Name (for Excel files):").grid(row=1, column=0, padx=10, pady=5, sticky="w")
        self.entries['sheet_name'] = ttk.Entry(input_frame)
        self.entries['sheet_name'].grid(row=1, column=1, padx=10, pady=5, sticky="ew")

        ttk.Checkbutton(input_frame, text="Process .txt files", variable=self.txt_enabled_var, command=self.toggle_input_txt_delimiter_field).grid(row=2, column=0, padx=10, pady=5, sticky="w")
        
        ttk.Label(input_frame, text="Delimiter for TXT files (use \\t for tab):").grid(row=3, column=0, padx=10, pady=5, sticky="w")
        self.entries['txt_delimiter'] = ttk.Entry(input_frame)
        self.entries['txt_delimiter'].grid(row=3, column=1, padx=10, pady=5, sticky="ew")

        ttk.Label(input_frame, text="Header Row Number:").grid(row=4, column=0, padx=10, pady=5, sticky="w")
        self.entries['header_row'] = ttk.Entry(input_frame)
        self.entries['header_row'].grid(row=4, column=1, padx=10, pady=5, sticky="ew")

        ttk.Label(input_frame, text="Data Start Row Number:").grid(row=5, column=0, padx=10, pady=5, sticky="w")
        self.entries['data_start_row'] = ttk.Entry(input_frame)
        self.entries['data_start_row'].grid(row=5, column=1, padx=10, pady=5, sticky="ew")
        
        ttk.Label(input_frame, text="Columns to extract (comma-separated):").grid(row=6, column=0, padx=10, pady=5, sticky="w")
        self.entries['columns_to_extract'] = ttk.Entry(input_frame)
        self.entries['columns_to_extract'].grid(row=6, column=1, padx=10, pady=5, sticky="ew")
        input_frame.columnconfigure(1, weight=1)

        # --- Merge Settings ---
        self.merge_frame = ttk.LabelFrame(self.scrollable_frame, text="Merge Settings")
        self.merge_frame.pack(padx=10, pady=10, fill="x")
        
        ttk.Checkbutton(self.merge_frame, text="Enable Merge with Lookup File", variable=self.merge_enabled_var, command=self.toggle_merge_fields).grid(row=0, column=0, columnspan=3, padx=10, pady=5, sticky="w")
        
        ttk.Label(self.merge_frame, text="Lookup File Path:").grid(row=1, column=0, padx=10, pady=5, sticky="w")
        self.entries['lookup_file_path'] = ttk.Entry(self.merge_frame, width=60)
        self.entries['lookup_file_path'].grid(row=1, column=1, padx=10, pady=5, sticky="ew")
        ttk.Button(self.merge_frame, text="Browse...", command=self.browse_lookup_file).grid(row=1, column=2, padx=5, pady=5)

        ttk.Label(self.merge_frame, text="Lookup Header Row Number:").grid(row=2, column=0, padx=10, pady=5, sticky="w")
        self.entries['lookup_header_row'] = ttk.Entry(self.merge_frame)
        self.entries['lookup_header_row'].grid(row=2, column=1, padx=10, pady=5, sticky="ew")

        ttk.Label(self.merge_frame, text="Lookup Data Start Row Number:").grid(row=3, column=0, padx=10, pady=5, sticky="w")
        self.entries['lookup_data_start_row'] = ttk.Entry(self.merge_frame)
        self.entries['lookup_data_start_row'].grid(row=3, column=1, padx=10, pady=5, sticky="ew")

        ttk.Checkbutton(self.merge_frame, text="Lookup file is a .txt file", variable=self.lookup_txt_enabled_var, command=self.toggle_lookup_txt_delimiter_field).grid(row=4, column=0, padx=10, pady=5, sticky="w")
        ttk.Label(self.merge_frame, text="Lookup TXT Delimiter (use \\t for tab):").grid(row=5, column=0, padx=10, pady=5, sticky="w")
        self.entries['lookup_txt_delimiter'] = ttk.Entry(self.merge_frame)
        self.entries['lookup_txt_delimiter'].grid(row=5, column=1, padx=10, pady=5, sticky="ew")

        ttk.Label(self.merge_frame, text="Source Key Column (in your files):").grid(row=6, column=0, padx=10, pady=5, sticky="w")
        self.entries['source_key_column'] = ttk.Entry(self.merge_frame)
        self.entries['source_key_column'].grid(row=6, column=1, padx=10, pady=5, sticky="ew")
        ttk.Label(self.merge_frame, text="Lookup Key Column (in lookup file):").grid(row=7, column=0, padx=10, pady=5, sticky="w")
        self.entries['lookup_key_column'] = ttk.Entry(self.merge_frame)
        self.entries['lookup_key_column'].grid(row=7, column=1, padx=10, pady=5, sticky="ew")
        ttk.Label(self.merge_frame, text="Columns to Add (comma-separated):").grid(row=8, column=0, padx=10, pady=5, sticky="w")
        self.entries['lookup_columns_to_add'] = ttk.Entry(self.merge_frame)
        self.entries['lookup_columns_to_add'].grid(row=8, column=1, padx=10, pady=5, sticky="ew")
        self.merge_frame.columnconfigure(1, weight=1)

        # --- Output Settings ---
        output_frame = ttk.LabelFrame(self.scrollable_frame, text="Output Settings")
        output_frame.pack(padx=10, pady=10, fill="x")
        ttk.Label(output_frame, text="Folder to save the report:").grid(row=0, column=0, padx=10, pady=5, sticky="w")
        self.entries['output_folder'] = ttk.Entry(output_frame, width=60)
        self.entries['output_folder'].grid(row=0, column=1, padx=10, pady=5, sticky="ew")
        ttk.Button(output_frame, text="Browse...", command=self.browse_output_folder).grid(row=0, column=2, padx=5, pady=5)
        ttk.Label(output_frame, text="Report Filename (without extension):").grid(row=1, column=0, padx=10, pady=5, sticky="w")
        self.entries['output_filename'] = ttk.Entry(output_frame)
        self.entries['output_filename'].grid(row=1, column=1, padx=10, pady=5, sticky="ew")
        ttk.Label(output_frame, text="Report Format:").grid(row=2, column=0, padx=10, pady=5, sticky="w")
        self.entries['output_format'] = ttk.Combobox(output_frame, values=['csv', 'xlsx'], state="readonly")
        self.entries['output_format'].grid(row=2, column=1, padx=10, pady=5, sticky="ew")
        output_frame.columnconfigure(1, weight=1)
        
        self.load_and_display_settings()
        ttk.Button(self.scrollable_frame, text="Save Settings", command=self.save_and_close).pack(pady=10)

    def load_and_display_settings(self):
        for key, widget in self.entries.items():
            if isinstance(widget, ttk.Combobox):
                widget.set(self.settings.get(key, "csv"))
            else:
                widget.delete(0, tk.END)
                widget.insert(0, str(self.settings.get(key, "")))
        self.toggle_merge_fields()
        self.toggle_input_txt_delimiter_field()
        self.toggle_lookup_txt_delimiter_field()

    def toggle_merge_fields(self):
        state = "normal" if self.merge_enabled_var.get() else "disabled"
        for child in self.merge_frame.winfo_children():
            if not isinstance(child, ttk.Checkbutton):
                child.configure(state=state)
        self.toggle_lookup_txt_delimiter_field()

    def toggle_input_txt_delimiter_field(self):
        state = "normal" if self.txt_enabled_var.get() else "disabled"
        self.entries['txt_delimiter'].configure(state=state)

    def toggle_lookup_txt_delimiter_field(self):
        merge_on = self.merge_enabled_var.get()
        lookup_txt_on = self.lookup_txt_enabled_var.get()
        state = "normal" if (merge_on and lookup_txt_on) else "disabled"
        self.entries['lookup_txt_delimiter'].configure(state=state)

    def browse_input_folder(self):
        folder_selected = filedialog.askdirectory()
        if folder_selected:
            self.entries['input_folder'].delete(0, tk.END)
            self.entries['input_folder'].insert(0, folder_selected)

    def browse_output_folder(self):
        folder_selected = filedialog.askdirectory()
        if folder_selected:
            self.entries['output_folder'].delete(0, tk.END)
            self.entries['output_folder'].insert(0, folder_selected)
    
    def browse_lookup_file(self):
        file_selected = filedialog.askopenfilename(filetypes=[("All supported files", "*.xlsx *.csv *.txt"), ("All files", "*.*")])
        if file_selected:
            self.entries['lookup_file_path'].delete(0, tk.END)
            self.entries['lookup_file_path'].insert(0, file_selected)

    def save_and_close(self):
        for key, widget in self.entries.items():
            self.settings[key] = widget.get()
        self.settings['enable_merge'] = self.merge_enabled_var.get()
        self.settings['enable_txt_processing'] = self.txt_enabled_var.get()
        self.settings['enable_lookup_txt'] = self.lookup_txt_enabled_var.get()

        for key in ["header_row", "data_start_row", "lookup_header_row", "lookup_data_start_row"]:
            try:
                value = self.settings[key]
                if str(value).strip():
                    self.settings[key] = int(value)
            except (ValueError, TypeError):
                messagebox.showerror("Error", f"Field '{key}' must be a number!")
                return
        
        if not self.settings.get('output_folder', '').strip():
            self.settings['output_folder'] = os.getcwd()
            self.entries['output_folder'].delete(0, tk.END)
            self.entries['output_folder'].insert(0, self.settings['output_folder'])
            
        save_settings(self.settings)
        messagebox.showinfo("Saved", "Settings saved successfully.")
        self.destroy()


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("File Combiner & Merger v4.7")
        self.geometry("700x500")

        top_frame = ttk.Frame(self)
        top_frame.pack(pady=10, padx=10, fill="x")

        self.settings_btn = ttk.Button(top_frame, text="Settings", command=self.open_settings)
        self.settings_btn.pack(side="left", padx=5)

        self.start_btn = ttk.Button(top_frame, text="Start", command=self.start_processing_thread)
        self.start_btn.pack(side="left", padx=5)

        self.log_area = scrolledtext.ScrolledText(self, wrap=tk.WORD, state='disabled')
        self.log_area.pack(pady=10, padx=10, expand=True, fill="both")

    def open_settings(self):
        SettingsWindow(self)

    def log(self, message):
        self.log_area.configure(state='normal')
        self.log_area.insert(tk.END, message)
        self.log_area.configure(state='disabled')
        self.log_area.see(tk.END)

    def start_processing_thread(self):
        self.start_btn.config(state="disabled")
        self.log_area.config(state="normal")
        self.log_area.delete(1.0, tk.END)
        self.log_area.config(state="disabled")

        thread = threading.Thread(target=self.run_processing, daemon=True)
        thread.start()

    def run_processing(self):
        try:
            settings = load_settings()
            
            required_fields = ["input_folder", "output_folder", "sheet_name", "header_row", "data_start_row", "output_filename"]
            if settings.get('enable_txt_processing'):
                required_fields.append("txt_delimiter")
            if settings.get('enable_merge'):
                required_fields.extend(["lookup_file_path", "lookup_header_row", "lookup_data_start_row", "source_key_column", "lookup_key_column", "lookup_columns_to_add"])
                if settings.get('enable_lookup_txt'):
                    required_fields.append("lookup_txt_delimiter")

            missing_fields = [field for field in required_fields if not str(settings.get(field, '')).strip()]

            if missing_fields:
                error_message = f"Please fill in all required fields in Settings: {', '.join(missing_fields)}"
                self.log(f"ERROR: {error_message}\n")
                messagebox.showerror("Missing Settings", error_message)
                return

            process_files(settings, self.log)
        except Exception as e:
            self.log(f"CRITICAL ERROR: {e}\n")
            messagebox.showerror("Critical Error", str(e))
        finally:
            self.after(0, lambda: self.start_btn.config(state="normal"))

if __name__ == "__main__":
    app = App()
    app.mainloop()
