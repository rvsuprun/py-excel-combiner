import pandas as pd
import os
import glob

# --- НАСТРОЙКИ ---
# Укажите путь к папке, где лежат ваши Excel-файлы.
FOLDER_PATH = 'C:/Users/rvsup/py exel/files'

# Укажите точное имя листа, с которого нужно брать данные.
SHEET_NAME = 'Template'

# Номер строки, где находятся заголовки (ключи). 5-я строка имеет индекс 4.
HEADER_ROW = 3 

# Номер строки, с которой начинаются данные. 7-я строка имеет индекс 6.
DATA_START_ROW = 6

# ИЗМЕНЕНИЕ: Список имен колонок, которые нужно извлечь.
# Укажите точные названия, как они есть в 5-й строке вашего Excel файла.
# Если оставить список пустым (вот так: []), скрипт извлечет ВСЕ колонки.
COLUMNS_TO_EXTRACT = [
    'SKU', 
    'Title',
    'Product Type', 
    'List Price',
    'Your Price USD (Sell on Amazon, US)',
    'Parentage Level',
    'Parent SKU',
    'Product Id',
    'Handling Time (US)',
    'Size',
    'Item Length Longer Edge',
    'Item Width Shorter Edge',
    'Item Weight',
    'Item Package Length',
    'Item Package Width',
    'Item Package Height',
    'Package Weight'
]

# Имя итогового файла, в который будут собраны все данные.
OUTPUT_FILE = 'combined_data.csv'
# --- КОНЕЦ НАСТРОЕК ---


def process_excel_files(folder_path, sheet_name, header_row, data_start_row, columns_to_extract, output_file):
    """
    Обрабатывает все Excel-файлы в указанной папке, извлекает данные
    из указанных колонок и объединяет их в один файл.
    """
    print(f"--- Начинаю обработку файлов в папке: {folder_path} ---")
    
    excel_files = glob.glob(os.path.join(folder_path, "*.xlsm"))

    if not excel_files:
        print("ОШИБКА: В указанной папке не найдено ни одного .xlsm файла.")
        return

    print(f"Найдено файлов для обработки: {len(excel_files)}")
    
    all_data_frames = []

    for file in excel_files:
        print(f"\n-> Обрабатываю файл: {os.path.basename(file)}")
        try:
            df = pd.read_excel(file, sheet_name=sheet_name, header=None)

            if len(df) < data_start_row:
                print(f"  - ПРЕДУПРЕЖДЕНИЕ: В файле недостаточно строк для извлечения данных. Пропускаю.")
                continue

            column_names = df.iloc[header_row]
            data_df = df.iloc[data_start_row:].copy()
            data_df.columns = column_names
            data_df.reset_index(drop=True, inplace=True)
            
            # ИЗМЕНЕНИЕ: Логика фильтрации колонок
            if columns_to_extract:
                # Проверяем, какие из нужных колонок есть в текущем файле
                existing_cols = [col for col in columns_to_extract if col in data_df.columns]
                missing_cols = [col for col in columns_to_extract if col not in data_df.columns]
                
                if missing_cols:
                    print(f"  - ПРЕДУПРЕЖДЕНИЕ: В этом файле отсутствуют колонки: {', '.join(missing_cols)}")
                
                if not existing_cols:
                    print("  - ПРЕДУПРЕЖДЕНИЕ: Ни одна из указанных колонок не найдена в файле. Пропускаю.")
                    continue

                # Выбираем только те колонки, которые существуют
                data_df = data_df[existing_cols]

            data_df['source_file'] = os.path.basename(file)
            all_data_frames.append(data_df)
            print(f"  - Успешно извлечено {len(data_df)} строк из {len(data_df.columns)-1} колонок.")

        except Exception as e:
            print(f"  - ОШИБКА при обработке файла: {e}")

    if not all_data_frames:
        print("\nНе удалось извлечь данные ни из одного файла.")
        return

    print("\n--- Объединяю все данные... ---")
    final_df = pd.concat(all_data_frames, ignore_index=True)

    final_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"\n🎉 Готово! Все данные успешно объединены в один файл: {output_file}")
    print(f"Всего обработано строк: {len(final_df)}")


if __name__ == '__main__':
    # ИЗМЕНЕНИЕ: передаем новый параметр в функцию
    process_excel_files(FOLDER_PATH, SHEET_NAME, HEADER_ROW, DATA_START_ROW, COLUMNS_TO_EXTRACT, OUTPUT_FILE)