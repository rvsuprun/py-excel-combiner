import pandas as pd
import os
import glob
import json
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext

# --- КОНФИГУРАЦИЯ ---
CONFIG_FILE = 'config.json'

# --- Управление настройками ---

def load_settings():
    """Загружает настройки из файла JSON. Если файла нет, возвращает значения по умолчанию."""
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        # Значения по умолчанию
        return {
            "folder_path": "",
            "sheet_name": "Sheet1",
            "header_row": 5,
            "data_start_row": 7,
            "columns_to_extract": "Артикул, Цена, Количество",
            "output_file": "combined_data.csv"
        }

def save_settings(settings):
    """Сохраняет настройки в файл JSON."""
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(settings, f, indent=4)

# --- Основная логика обработки файлов ---

def process_excel_files(settings, log_callback):
    """
    Основная логика обработки файлов. Теперь принимает настройки и функцию для логирования.
    """
    folder_path = settings['folder_path']
    sheet_name = settings['sheet_name']
    header_row = settings['header_row'] - 1  # Pandas-индексация с 0
    data_start_row = settings['data_start_row'] - 1  # Pandas-индексация с 0
    columns_to_extract = [col.strip() for col in settings['columns_to_extract'].split(',') if col.strip()]
    output_file = settings['output_file']

    log_callback(f"--- Начинаю обработку файлов в папке: {folder_path} ---\n")
    
    # Сначала ищем .xlsm, если не нашли - ищем .xlsx
    excel_files = glob.glob(os.path.join(folder_path, "*.xlsm"))
    if not excel_files:
        excel_files = glob.glob(os.path.join(folder_path, "*.xlsx"))

    if not excel_files:
        log_callback("ОШИБКА: В указанной папке не найдено .xlsx или .xlsm файлов.\n")
        return

    log_callback(f"Найдено файлов для обработки: {len(excel_files)}\n")
    all_data_frames = []

    for i, file in enumerate(excel_files, 1):
        log_callback(f"\n[{i}/{len(excel_files)}] -> Обрабатываю файл: {os.path.basename(file)}\n")
        try:
            df = pd.read_excel(file, sheet_name=sheet_name, header=None)

            if len(df) < data_start_row:
                log_callback(f"  - ПРЕДУПРЕЖДЕНИЕ: В файле недостаточно строк. Пропускаю.\n")
                continue

            column_names = df.iloc[header_row]
            data_df = df.iloc[data_start_row:].copy()
            data_df.columns = column_names
            data_df.reset_index(drop=True, inplace=True)
            
            if columns_to_extract:
                existing_cols = [col for col in columns_to_extract if col in data_df.columns]
                missing_cols = [col for col in columns_to_extract if col not in data_df.columns]
                if missing_cols:
                    log_callback(f"  - ПРЕДУПРЕЖДЕНИЕ: Отсутствуют колонки: {', '.join(missing_cols)}\n")
                if not existing_cols:
                    log_callback("  - ПРЕДУПРЕЖДЕНИЕ: Ни одна из нужных колонок не найдена. Пропускаю.\n")
                    continue
                data_df = data_df[existing_cols]

            data_df['source_file'] = os.path.basename(file)
            all_data_frames.append(data_df)
            log_callback(f"  - Успешно извлечено {len(data_df)} строк.\n")

        except Exception as e:
            log_callback(f"  - ОШИБКА при обработке файла: {e}\n")

    if not all_data_frames:
        log_callback("\nНе удалось извлечь данные ни из одного файла.\n")
        return

    log_callback("\n--- Объединяю все данные... ---\n")
    final_df = pd.concat(all_data_frames, ignore_index=True)
    final_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    log_callback(f"\n🎉 Готово! Все данные объединены в файл: {output_file}\n")
    log_callback(f"Всего обработано строк: {len(final_df)}\n")
    messagebox.showinfo("Готово", f"Обработка завершена! Результат в файле {output_file}")


# --- Графический интерфейс (GUI) ---

class SettingsWindow(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        
        # ИЗМЕНЕНИЕ: Добавлены эти две строки, чтобы окно не "пряталось"
        self.transient(parent)
        self.grab_set()

        self.title("Настройки")
        self.geometry("500x300")
        self.parent = parent
        self.settings = load_settings()

        # Создаем поля для ввода
        self.entries = {}
        fields = {
            "folder_path": "Путь к папке с файлами:",
            "sheet_name": "Имя листа в Excel:",
            "header_row": "Номер строки с заголовками:",
            "data_start_row": "Номер строки начала данных:",
            "columns_to_extract": "Нужные колонки (через запятую):",
            "output_file": "Имя итогового файла:"
        }

        for i, (key, text) in enumerate(fields.items()):
            label = ttk.Label(self, text=text)
            label.grid(row=i, column=0, padx=10, pady=5, sticky="w")
            
            entry = ttk.Entry(self, width=50)
            entry.grid(row=i, column=1, padx=10, pady=5, sticky="ew")
            entry.insert(0, self.settings.get(key, ""))
            self.entries[key] = entry
        
        # Кнопка выбора папки
        browse_btn = ttk.Button(self, text="Обзор...", command=self.browse_folder)
        browse_btn.grid(row=0, column=2, padx=5, pady=5)

        # Кнопка сохранения
        save_btn = ttk.Button(self, text="Сохранить", command=self.save_and_close)
        save_btn.grid(row=len(fields), column=1, pady=20)
        
        self.columnconfigure(1, weight=1)

    def browse_folder(self):
        folder_selected = filedialog.askdirectory()
        if folder_selected:
            self.entries['folder_path'].delete(0, tk.END)
            self.entries['folder_path'].insert(0, folder_selected)

    def save_and_close(self):
        for key, entry in self.entries.items():
            # Для числовых полей преобразуем в int
            if key in ["header_row", "data_start_row"]:
                try:
                    self.settings[key] = int(entry.get())
                except ValueError:
                    messagebox.showerror("Ошибка", f"Поле '{key}' должно быть числом!")
                    return
            else:
                self.settings[key] = entry.get()
        save_settings(self.settings)
        messagebox.showinfo("Сохранено", "Настройки успешно сохранены.")
        self.destroy()


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Объединитель Excel файлов")
        self.geometry("700x500")

        # Создаем фрейм для кнопок
        top_frame = ttk.Frame(self)
        top_frame.pack(pady=10, padx=10, fill="x")

        self.settings_btn = ttk.Button(top_frame, text="Настройки", command=self.open_settings)
        self.settings_btn.pack(side="left", padx=5)

        self.start_btn = ttk.Button(top_frame, text="Старт", command=self.start_processing_thread)
        self.start_btn.pack(side="left", padx=5)

        # Создаем текстовое поле для вывода логов
        self.log_area = scrolledtext.ScrolledText(self, wrap=tk.WORD, state='disabled')
        self.log_area.pack(pady=10, padx=10, expand=True, fill="both")

    def open_settings(self):
        SettingsWindow(self)

    def log(self, message):
        """Безопасный вывод сообщений в текстовое поле из любого потока."""
        self.log_area.configure(state='normal')
        self.log_area.insert(tk.END, message)
        self.log_area.configure(state='disabled')
        self.log_area.see(tk.END) # Автопрокрутка вниз

    def start_processing_thread(self):
        """Запускает обработку в отдельном потоке, чтобы интерфейс не зависал."""
        self.start_btn.config(state="disabled")
        self.log_area.config(state="normal")
        self.log_area.delete(1.0, tk.END) # Очищаем лог перед запуском
        self.log_area.config(state="disabled")

        # Создаем и запускаем поток
        thread = threading.Thread(target=self.run_processing, daemon=True)
        thread.start()

    def run_processing(self):
        """Функция, которая выполняется в потоке."""
        try:
            settings = load_settings()
            if not settings.get('folder_path'):
                self.log("ОШИБКА: Путь к папке не указан. Зайдите в Настройки и выберите папку.\n")
                messagebox.showerror("Ошибка", "Путь к папке не указан. Зайдите в Настройки и выберите папку.")
                return

            process_excel_files(settings, self.log)
        except Exception as e:
            self.log(f"КРИТИЧЕСКАЯ ОШИБКА: {e}\n")
            messagebox.showerror("Критическая ошибка", str(e))
        finally:
            self.start_btn.config(state="normal") # Возвращаем кнопку в активное состояние


if __name__ == "__main__":
    app = App()
    app.mainloop()