import json
import os
from datetime import datetime
import configparser
import sys
import win32api
import glob
import subprocess
import platform
import openpyxl

DEFAULT_CONFIG = {
    "host": "localhost",
    "database": "C:/REGOS BASE/REGOS.FDB",
    "user": "SYSDBA",
    "password": "masterkey",
    "price_type": 1,
    "check_time": 10,
    "divider_price": 1,
    "use_articul": True,
    "plu_file_path": r"C:\REGOS BASE\plu",
    "scales_config_path": r"C:\Program Files (x86)\ШТРИХ-М\ШТРИХ-ПРИНТ\Automatic Loader\TrayLoader.ini",
    "only_changed_items": True,
    "handle_big_price": {
        "active": True,
        "divider": 100,
    },
    "units": [
        {
            "name": "Весовой", # Весовой: 0, штучный: 1
            "id": 2,
            "type": 0,
        },
        {
            "name": "Штучный",
            "id": 1,
            "type": 1, # Весовой: 0, штучный: 1
        },
    ],
}

README_CONTENT = """
Программа для загрузки PLU из Regos (firebird база данных) в файл. Загружает только новые или изменённые товары.
# Руководство по настройке конфигурации

## Описание параметров конфигурации

Файл конфигурации содержит следующие параметры:

### Параметры подключения к базе данных
- `host` - адрес сервера базы данных Firebird (по умолчанию: "localhost")
- `database` - путь к файлу базы данных (по умолчанию: "C:/REGOS BASE/REGOS.FDB"). 
Путь должен быть записан латинскими буквами и 
использовать только символ '/' для разделения частей пути
- `user` - имя пользователя для подключения к базе данных (по умолчанию: "SYSDBA")
- `password` - пароль для подключения к базе данных (по умолчанию: "masterkey")

### Параметры работы с ценами
- `price_type` - тип цены (по умолчанию: 1). 
- `divider_price` - делитель цены (по умолчанию: 1)

### Параметры синхронизации и проверки
- `check_time` - период проверки в секундах (по умолчанию: 10)
- `use_articul` - использовать артикул (по умолчанию: True)

### Пути к файлам
- `plu_file_path` - путь к файлу PLU (по умолчанию: "C:\\\\REGOS BASE\\\\plu"). 
использовать только символ '\\\\' для разделения частей пути
- `scales_config_path` - путь к конфигурационному файлу весов (по умолчанию:
"C:\\\\Program Files (x86)\\\\ШТРИХ-М\\\\ШТРИХ-ПРИНТ\\\\Automatic Loader\\\\TrayLoader.ini"). 
использовать только символ '\\\\' для разделения частей пути

### Настройки единиц измерения
В системе настроены два типа единиц измерения:

1. Весовой товар:
   - `name`: "Весовой"
   - `id`: 2 - Посмотреть код единицы измерения в программе Regos.
   - `type`: 0 (0 соответствует весовому товару)

2. Штучный товар:
   - `name`: "Штучный"
   - `id`: 1 - Посмотреть код единицы измерения в программе Regos.
   - `type`: 1 (1 соответствует штучному товару)

## Как изменить конфигурацию

Для изменения настроек отредактируйте файл конфигурации, сохраняя его структуру и формат JSON. 
После внесения изменений перезапустите приложение для применения новых настроек.

## Примечания
- Все пути к файлам должны быть указаны в полном формате
- Убедитесь, что у программы есть права доступа к указанным файлам и папкам
- Используйте create_service.bat файл для создания сервиса, 
папка программа должна находиться в папке 'Program Files (x86)'. 
Запустите файл от имени администратора.
"""

today = datetime.now().strftime("%d-%m-%Y")
log_file = f"logs/log-{today}.log"
os.makedirs(os.path.dirname(log_file), exist_ok=True)

def get_date():
    now = datetime.now()
    return now.strftime("%m/%d/%Y %H:%M:%S")

def write_log_file(text):
    with open(log_file, "a", encoding='utf-8') as file:
        formatted_text = f"{get_date()} - {text}\n"
        file.write(formatted_text)
        print(formatted_text)

def configure_settings(data_dict=DEFAULT_CONFIG, filename="config.json"):
    if os.path.exists(filename):
        try:
            with open(filename, 'r', encoding='utf-8') as json_file:
                data_dict = json.load(json_file)
            return data_dict
        except FileNotFoundError:
            write_log_file(f"Error: File '{filename}' not found")

        except json.JSONDecodeError:
            write_log_file(f"Error: File '{filename}' contains invalid JSON")
            os.remove(filename)

        except Exception as e:
            write_log_file(f"Error reading JSON file: {e}")
            os.remove(filename)


    try:
        with open(filename, 'w', encoding='utf-8', errors="replace") as json_file:
            json.dump(data_dict, json_file, indent=4, ensure_ascii=False)
    except Exception as e:
        write_log_file(f"Error writing to JSON file: {e}")
    else:
        return data_dict

def int_to_ip(ip_int):
    """
    Convert a signed 32-bit integer to an IP address string

    Args:
        ip_int (int): The IP address as a signed 32-bit integer

    Returns:
        str: The IP address in standard format (xxx.xxx.xxx.xxx)
    """
    # Convert from negative integer to unsigned 32-bit integer
    if ip_int < 0:
        ip_int = ip_int + 2 ** 32

    # Extract each octet
    octet1 = (ip_int >> 24) & 255
    octet2 = (ip_int >> 16) & 255
    octet3 = (ip_int >> 8) & 255
    octet4 = ip_int & 255

    return f"{octet4}.{octet3}.{octet2}.{octet1}"


def extract_ip_addresses_from_ini_and_create_path(ini_file_path: str, plu_file_path: str, ip_addresses_dict: dict, save_type: str = "old") -> None:
    """
    Extract IP addresses from the given INI file, create path and return them as a list
    in the format [192-168-1-201, 192-168-1-202, ...]

    Args:
        ini_file_path (str): Path to the INI file
        plu_file_path (str): Path to the plu file
        ip_addresses_dict (dict): Dictionary of IP addresses extracted from INI file.
        save_type (str, optional): "new" if for creating new file, "old" using old plu file. Defaults to "old".

    Returns:
        None
    """
    # Create a ConfigParser object
    config = configparser.ConfigParser()

    # Read the INI file
    config.read(ini_file_path)


    # Loop through all sections in the INI file
    for section in config.sections():
        # Check if the section starts with "Device." and has an IP entry
        if section.startswith("Device.") and "IP" in config[section]:
            # Get the IP value as an integer
            ip_int = config.getint(section, "IP")

            # Convert the integer to IP address
            ip_address = int_to_ip(ip_int)
            formatted_plu_path = fr"{plu_file_path}\{ip_address.replace(".", "-")}.txt"
            # Add the formatted IP to the list
            ip_addresses_dict[ip_address] = {"path": formatted_plu_path, "type": save_type}

def create_arg_query(units_data: list, latest_changes: dict | None, only_changed_items: bool = True) -> str:
    list_data = []
    for value in units_data:
        list_data.append(value["id"])

    sql_args = ""
    if len(list_data) > 1:
        sql_args += f"AND I.ITM_UNIT IN {tuple(list_data)}"
    elif len(list_data) == 1:
        sql_args += f"AND I.ITM_UNIT = {list_data[0]}"
    else:
        sys.exit(1)

    if latest_changes and only_changed_items:
        item_last_change = datetime.strftime(latest_changes["items"], "%Y-%m-%d %H:%M:%S")
        price_last_change = datetime.strftime(latest_changes["prices"], "%Y-%m-%d %H:%M:%S")
        sql_args += f" AND (I.ITM_LAST_UPDATE > '{item_last_change}' OR P.PRC_LAST_UPDATE > '{price_last_change}')"
    return sql_args


def get_units_type(units: list):
    units_dict = {}
    for unit_info in units:
        units_dict[unit_info["id"]] = unit_info["type"]
    return units_dict

def find_available_plu_numbers(numbers, count):
    if not numbers:
        return [i for i in range(1, count + 1)]
    num_set = set(numbers)

    missing_numbers = []

    current = 1

    while len(missing_numbers) < count and current < 100000:
        if current not in num_set:
            missing_numbers.append(current)
        current += 1

    return missing_numbers

def find_available_plu(numbers):
    for num in range(1, 100000):
        if num not in numbers:
            return num

def combine_plu_lists(old_plu_data_list: list, new_plu_data_list: list, articul_dict: dict | None,
                      used_plus: dict | None) -> list:
    combined_data = new_plu_data_list
    new_plu_dict = {}

    for item in new_plu_data_list:
        parts = item.split(';')
        unique_id = int(parts[7])
        new_plu_dict[unique_id] = item


    for old_plu in old_plu_data_list:
        parts = old_plu.split(';')
        unique_id = int(parts[7])
        if unique_id not in new_plu_dict:
            combined_data.append(old_plu)

    return combined_data

def get_short_path_name(path):
    try:
        return win32api.GetShortPathName(path)
    except Exception as e:
        write_log_file(f"Error getting short path name: {e}")
        return path


def save_readme_if_not_exists(readme_content=README_CONTENT, readme_path="README.md"):
    """
    Save a README file if it does not already exist.

    Parameters:
    readme_content (str): The content to write to the README file
    readme_path (str): The path where the README file should be saved (default: "README.md")

    Returns:
    bool: True if the file was created, False if it already existed or if an error occurred
    """
    try:
        # Check if the file already exists
        if os.path.exists(readme_path):
            write_log_file(f"README file already exists at '{readme_path}'. No changes made.")
            return False

        # Create the directory if it doesn't exist
        directory = os.path.dirname(readme_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)

        # Write the README file
        with open(readme_path, 'w', encoding='utf-8') as readme_file:
            readme_file.write(readme_content)

        write_log_file(f"README file successfully created at '{readme_path}'.")
        return True

    except Exception as e:
        write_log_file(f"Error creating README file: {e}")
        return False

def get_key_by_value(dictionary, value):
    for val in dictionary.values():
        if val["code"] == value:
            return val
    return None

def delete_txt_files(folder_path):
    """
    Delete all .txt files from the specified folder.

    Args:
        folder_path (str): The path to the folder containing .txt files

    Returns:
        int: Number of files deleted
    """
    # Create a pattern to match all .txt files in the folder
    pattern = os.path.join(folder_path, "*.txt")

    # Find all .txt files
    txt_files = glob.glob(pattern)

    # Count how many files we'll delete
    count = len(txt_files)

    # Delete each file
    for file_path in txt_files:
        try:
            os.remove(file_path)
            write_log_file(f"Deleted: {file_path}")
        except Exception as e:
            write_log_file(f"Error deleting {file_path}: {e}")

    write_log_file(f"Total .txt files deleted: {count}")
    return count


def ping_device(ip_address, count=3, timeout=2):
    """
    Ping a device to check if it exists and is reachable on the network.

    Args:
        ip_address (str): The IP address or hostname of the device
        count (int): Number of ping packets to send (default: 1)
        timeout (int): Timeout in seconds (default: 2)

    Returns:
        bool: True if device responds to ping, False otherwise
    """
    # Determine the ping command based on the operating system
    param = '-n' if platform.system().lower() == 'windows' else '-c'
    timeout_param = '-w' if platform.system().lower() == 'windows' else '-W'

    # Construct the ping command
    command = ['ping', param, str(count), timeout_param, str(timeout), ip_address]

    try:
        # Run the ping command and capture the output
        # subprocess.check_output will raise an exception if the command fails
        subprocess.check_output(command, stderr=subprocess.STDOUT, universal_newlines=True)
        return True
    except subprocess.CalledProcessError:
        # If ping fails, return False
        return False


def write_tuples_to_excel(tuples_list, filename='output.xlsx', sheet_name='Sheet1'):
    """
    Write a list of tuples to an Excel file.

    Parameters:
    - tuples_list (list): List of tuples to be written to Excel
    - filename (str, optional): Name of the Excel file to create. Defaults to 'output.xlsx'
    - sheet_name (str, optional): Name of the worksheet. Defaults to 'Sheet1'

    Returns:
    - str: Path to the created Excel file
    """
    # Create a new workbook and select the active sheet
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = sheet_name

    # Write the tuples to the Excel sheet
    for row_index, tuple_data in enumerate(tuples_list, start=1):
        for col_index, value in enumerate(tuple_data, start=1):
            sheet.cell(row=row_index, column=col_index, value=value)

    # Save the workbook
    workbook.save(filename)

    return filename


