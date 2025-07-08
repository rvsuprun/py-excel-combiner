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
            "folder_path": "",
            "sheet_name": "Sheet1",
            "header_row": 5,
            "data_start_row": 7,
            "columns_to_extract": "SKU, Price, Quantity",
            "output_file": "combined_data.csv"
        }

def save_settings(settings):
    """Saves settings to a JSON file."""
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(settings, f, indent=4)

# --- Core File Processing Logic ---

def process_excel_files(settings, log_callback):
    """
    Core logic to process files. Now accepts settings and a logging callback function.
    """
    folder_path = settings['folder_path']
    sheet_name = settings['sheet_name']
    header_row = settings['header_row'] - 1  # Pandas is 0-indexed
    data_start_row = settings['data_start_row'] - 1  # Pandas is 0-indexed
    columns_to_extract = [col.strip() for col in settings['columns_to_extract'].split(',') if col.strip()]
    output_file = settings['output_file']

    log_callback(f"--- Starting to process files in folder: {folder_path} ---\n")
    
    excel_files = glob.glob(os.path.join(folder_path, "*.xlsm"))
    if not excel_files:
        excel_files = glob.glob(os.path.join(folder_path, "*.xlsx"))

    if not excel_files:
        log_callback("ERROR: No .xlsx or .xlsm files found in the specified folder.\n")
        return

    log_callback(f"Found {len(excel_files)} files to process.\n")
    all_data_frames = []

    for i, file in enumerate(excel_files, 1):
        log_callback(f"\n[{i}/{len(excel_files)}] -> Processing file: {os.path.basename(file)}\n")
        try:
            df = pd.read_excel(file, sheet_name=sheet_name, header=None)

            if len(df) < data_start_row:
                log_callback(f"  - WARNING: Not enough rows in the file to extract data. Skipping.\n")
                continue

            column_names = df.iloc[header_row]
            data_df = df.iloc[data_start_row:].copy()
            data_df.columns = column_names
            data_df.reset_index(drop=True, inplace=True)
            
            if columns_to_extract:
                existing_cols = [col for col in columns_to_extract if col in data_df.columns]
                missing_cols = [col for col in columns_to_extract if col not in data_df.columns]
                if missing_cols:
                    log_callback(f"  - WARNING: These columns are missing in this file: {', '.join(missing_cols)}\n")
                if not existing_cols:
                    log_callback("  - WARNING: None of the specified columns were found. Skipping file.\n")
                    continue
                data_df = data_df[existing_cols]

            data_df['source_file'] = os.path.basename(file)
            all_data_frames.append(data_df)
            log_callback(f"  - Successfully extracted {len(data_df)} rows.\n")

        except Exception as e:
            log_callback(f"  - ERROR while processing file: {e}\n")

    if not all_data_frames:
        log_callback("\nCould not extract data from any file.\n")
        return

    log_callback("\n--- Combining all data... ---\n")
    final_df = pd.concat(all_data_frames, ignore_index=True)
    final_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    log_callback(f"\n🎉 Done! All data has been combined into file: {output_file}\n")
    log_callback(f"Total rows processed: {len(final_df)}\n")
    messagebox.showinfo("Done", f"Processing complete! Result saved in {output_file}")


# --- Graphical User Interface (GUI) ---

class SettingsWindow(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        
        self.transient(parent)
        self.grab_set()

        self.title("Settings")
        self.geometry("550x300")
        self.parent = parent
        self.settings = load_settings()

        self.entries = {}
        fields = {
            "folder_path": "Path to folder with files:",
            "sheet_name": "Sheet Name in Excel:",
            "header_row": "Header Row Number:",
            "data_start_row": "Data Start Row Number:",
            "columns_to_extract": "Columns to extract (comma-separated):",
            "output_file": "Output Filename:"
        }

        for i, (key, text) in enumerate(fields.items()):
            label = ttk.Label(self, text=text)
            label.grid(row=i, column=0, padx=10, pady=5, sticky="w")
            
            entry = ttk.Entry(self, width=60)
            entry.grid(row=i, column=1, padx=10, pady=5, sticky="ew")
            entry.insert(0, self.settings.get(key, ""))
            self.entries[key] = entry
        
        browse_btn = ttk.Button(self, text="Browse...", command=self.browse_folder)
        browse_btn.grid(row=0, column=2, padx=5, pady=5)

        save_btn = ttk.Button(self, text="Save", command=self.save_and_close)
        save_btn.grid(row=len(fields), column=1, pady=20)
        
        self.columnconfigure(1, weight=1)

    def browse_folder(self):
        folder_selected = filedialog.askdirectory()
        if folder_selected:
            self.entries['folder_path'].delete(0, tk.END)
            self.entries['folder_path'].insert(0, folder_selected)

    def save_and_close(self):
        for key, entry in self.entries.items():
            if key in ["header_row", "data_start_row"]:
                try:
                    self.settings[key] = int(entry.get())
                except ValueError:
                    messagebox.showerror("Error", f"Field '{key}' must be a number!")
                    return
            else:
                self.settings[key] = entry.get()
        save_settings(self.settings)
        messagebox.showinfo("Saved", "Settings saved successfully.")
        self.destroy()


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Excel File Combiner")
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
            if not settings.get('folder_path'):
                self.log("ERROR: Folder path is not specified. Please go to Settings and select a folder.\n")
                messagebox.showerror("Error", "Folder path is not specified. Please go to Settings and select a folder.")
                return

            process_excel_files(settings, self.log)
        except Exception as e:
            self.log(f"CRITICAL ERROR: {e}\n")
            messagebox.showerror("Critical Error", str(e))
        finally:
            self.start_btn.config(state="normal")

if __name__ == "__main__":
    app = App()
    app.mainloop()