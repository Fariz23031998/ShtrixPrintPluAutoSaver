import fdb
import time
import os

from helper import configure_settings, write_log_file, get_units_type, extract_ip_addresses_from_ini_and_create_path, \
    combine_plu_lists, find_available_plu_numbers, create_arg_query, get_short_path_name, save_readme_if_not_exists, \
    delete_txt_files

# pyinstaller command: pyinstaller --onefile --name=ShtrixPrintPluAutoSaver save.py

config = configure_settings()
price_type = config["price_type"]
host = config["host"]
database = config["database"]
user = config["user"]
password = config["password"]
divider_price = config["divider_price"]
units = config["units"]
use_articul = config["use_articul"]
plu_file_path = config["plu_file_path"]
check_time = config["check_time"]
scales_config_path = config["scales_config_path"]
units_dict = get_units_type(units=units)
create_file_only_at_changes = config["create_file_only_at_changes"]
handle_big_price = config["handle_big_price"]

class SaveDataToTXT:
    def __init__(self):
        self.fdb_conn = None
        self.last_sync = 0
        self.path = get_short_path_name(database)
        self.connection_status = False
        self.last_change_dict = {}
        self.last_changes_timestamp = 0
        self.used_plus = {}
        self.new_file_creation = False
        self.temp_articul_dict = {}
        os.makedirs(plu_file_path, exist_ok=True)
        save_readme_if_not_exists()
        delete_txt_files(plu_file_path)

    def connect_fdb(self):
        try:
            self.fdb_conn = fdb.connect(
                host=host,
                database=self.path,
                user=user,
                password=password,
                charset='utf-8',
            )
        except fdb.fbcore.DatabaseError:
            write_log_file(f"Can't connect to the Firebird.")
            self.connection_status = False
            return False
        else:
            self.connection_status = True
            write_log_file("Connected to the Firebird.")
            return True

    def check_cash_status(self) -> int:
        # 0: Didn't connect to fdb, 1: database changed, 2: connected, but database didn't change
        query_check_sync = """
        SELECT 
            S.SST_DATE, 
            S.SST_STATUS
        FROM SYS_SYNC_PROCCESS_REF S
        WHERE S.SST_STATUS = 1
        """
        try:
            fdb_cursor = self.fdb_conn.cursor()
            fdb_cursor.execute(query_check_sync)
        except AttributeError as e:
            write_log_file(f"Error: {e}")
            self.connection_status = False
            return 0
        except Exception as e:
            self.connection_status = False
            write_log_file(f"Error: {e}")
            return 0
        sync_process = fdb_cursor.fetchall()
        sync_value = 0
        for sync in sync_process:
            timestamp = sync[0].timestamp()
            if timestamp > sync_value:
                sync_value = timestamp

        if sync_value > self.last_sync:
            self.last_sync = sync_value
            return 1
        else:
            return 2

    def check_last_changes(self):
        try:
            query_items_last_changes = """
            SELECT FIRST 1
                ITM_LAST_UPDATE
            FROM CTLG_ITM_ITEMS_REF
            ORDER BY ITM_LAST_UPDATE DESC
            """
            fdb_cursor = self.fdb_conn.cursor()
            fdb_cursor.execute(query_items_last_changes)
            items_last_update = fdb_cursor.fetchone()[0]
            items_last_update_timestamp = items_last_update.timestamp()

            query_prices_last_changes = """
            SELECT FIRST 1
                PRC_LAST_UPDATE
            FROM CTLG_ITM_PRICES_REF
            ORDER BY PRC_LAST_UPDATE DESC
            """
            fdb_cursor.execute(query_prices_last_changes)
            prices_last_update = fdb_cursor.fetchone()[0]
            prices_last_update_timestamp = prices_last_update.timestamp()


        except Exception as e:
            write_log_file(f"Error: {e}")
            self.connection_status = False
            return False
        else:
            latest = max(items_last_update_timestamp, prices_last_update_timestamp)
            if self.last_changes_timestamp < latest:
                self.last_changes_timestamp = latest
                return items_last_update, prices_last_update
            else:
                return False


    def fetch_items(self):
        fetch_item_args = create_arg_query(units, self.last_change_dict)
        query_fetch_items = f"""
        SELECT FIRST 22700 
            I.ITM_ID, 
            I.ITM_CODE,
            I.ITM_ARTICUL, 
            I.ITM_NAME, 
            I.ITM_UNIT, 
            I.ITM_GROUP, 
            P.PRC_VALUE
        FROM CTLG_ITM_ITEMS_REF I
        LEFT JOIN CTLG_ITM_PRICES_REF P ON I.ITM_ID = P.PRC_ITEM
        WHERE I.ITM_DELETED_MARK = 0 
            AND P.PRC_PRICE_TYPE = ? 
            AND P.PRC_VALUE <> 0 
            {fetch_item_args}
            ORDER BY I.ITM_ID ASC
        """

        try:
            fdb_cursor = self.fdb_conn.cursor()

            fdb_cursor.execute(query_fetch_items, (1, ))
            data = fdb_cursor.fetchall()

        except Exception as e:
            write_log_file(f"Error: {e}")
            self.connection_status = False
            return False
        else:
            return data

    def fetch_articuls_info(self):
        query_articuls_info = f"""
        SELECT FIRST 22700 
            I.ITM_CODE,
            I.ITM_ARTICUL
        FROM CTLG_ITM_ITEMS_REF I
        WHERE I.ITM_DELETED_MARK = 0 
          AND I.ITM_ARTICUL IS NOT NULL
          AND I.ITM_ARTICUL SIMILAR TO '[1-9][0-9]*'  -- Only contains digits
          AND I.ITM_ARTICUL NOT LIKE '%.%'       -- No decimal points
          AND NOT EXISTS (
              SELECT 1 FROM CTLG_ITM_ITEMS_REF I2
              WHERE I2.ITM_ARTICUL = I.ITM_ARTICUL
              AND I2.ITM_CODE <> I.ITM_CODE
          ) 
        ORDER BY I.ITM_CODE ASC
        """
        try:
            fdb_cursor = self.fdb_conn.cursor()

            fdb_cursor.execute(query_articuls_info, (1, ))
            data = fdb_cursor.fetchall()

        except Exception as e:
            write_log_file(f"Error: {e}")
            self.connection_status = False
            return False
        else:
            if data:
                return {item[1]: item[0] for item in data if int(item[1]) < 23000}
            else:
                return None


    def save_string_to_file(self, text, file_path):
        with open(file_path, 'w', encoding='windows-1251', errors="replace") as file:
            file.write(text)


    def save_to_txt(self):
        last_changes = self.check_last_changes() if create_file_only_at_changes else False
        if not last_changes:
            write_log_file(f"DB wasn't changed")
            return False

        articuls_data = self.fetch_articuls_info() if use_articul else None
        if articuls_data and articuls_data != self.temp_articul_dict:
            self.temp_articul_dict = articuls_data
            self.new_file_creation = True
            self.last_change_dict = {}
            self.used_plus = {}

        data = self.fetch_items()
        if not data:
            write_log_file("No items to save")
            return False

        plu_data = []
        if use_articul and articuls_data:
            for key, value in articuls_data.items():
                self.used_plus[value] = {"code": value, "plu": int(key), "is_articul": True}

            used_plu_list = [plu["plu"] for plu in self.used_plus.values()]
            available_plu_list = find_available_plu_numbers(numbers=used_plu_list, count=len(data))
            available_plu_pos = 0
            for item in data:
                unit_type = units_dict.get(item[4])
                code = item[1]
                price = item[6] / divider_price

                if price >= 1000000:
                    if handle_big_price['active']:
                        price = price / handle_big_price['divider']
                    else:
                        continue

                if articuls_data and item[2] in articuls_data.keys():
                    plu_data.append(
                        f"{int(item[2])};{item[3]};;{price};0;0;0;{code};0;0;;01.01.01;{unit_type}")

                else:
                    # In this part of code there's no valid articul has been detected
                    available_plu = available_plu_list[available_plu_pos]
                    used_plu_val = self.used_plus.get(code)
                    if used_plu_val and not used_plu_val["is_articul"]:
                        # PLU was uploaded before and wasn't articul
                        plu_data.append(
                            f"{used_plu_val['plu']};{item[3]};;{price};0;0;0;{code};0;0;;01.01.01;{unit_type}")

                    else:
                        # PLU wasn't uploaded, it's purely new and not articul
                        plu_data.append(
                            f"{available_plu};{item[3]};;{price};0;0;0;{code};0;0;;01.01.01;{unit_type}")
                        self.used_plus[code] = {"code": code, "plu": available_plu, "is_articul": False}
                        available_plu_pos += 1

        else:
            used_plu_list = [plu["plu"] for plu in self.used_plus.values()]
            available_plu_list = find_available_plu_numbers(numbers=used_plu_list, count=len(data))
            available_plu_pos = 0
            for item in data:
                unit_type = units_dict.get(item[4])
                code = item[1]
                price = item[6] / divider_price
                if price >= 1000000:
                    if handle_big_price['active']:
                        price = price / handle_big_price['divider']
                    else:
                        continue
                available_plu = available_plu_list[available_plu_pos]
                if code not in self.used_plus.keys():
                    plu_data.append(
                        f"{available_plu};{item[3]};;{price};0;0;0;{code};0;0;;01.01.01;{unit_type}")
                    self.used_plus[code] = {"code": code, "plu": available_plu, "is_articul": False}
                    available_plu_pos += 1

                else:
                    plu_data.append(
                        f"{self.used_plus[code]["plu"]};{item[3]};;{price};0;0;0;{code};0;0;;01.01.01;{unit_type}")


        string_data = "\n".join(plu_data)
        plu_files_path = extract_ip_addresses_from_ini_and_create_path(
            ini_file_path=scales_config_path,
            plu_file_path=plu_file_path
        )
        scale_q = len(plu_files_path)
        creation_q = 0
        for plu_path in plu_files_path:
            if not os.path.exists(plu_path) or self.new_file_creation:
                write_log_file(f"{len(plu_data)} PLUs was saved into '{plu_path}'")
                self.save_string_to_file(string_data, plu_path)

                creation_q += 1
                if creation_q >= scale_q:
                    self.new_file_creation = False

            else:
                with open(plu_path, 'r', encoding='windows-1251') as plu_file:
                    old_plu_list = plu_file.read().splitlines()

                new_item_q = len(plu_data)
                combined_plu_data = combine_plu_lists(old_plu_data_list=old_plu_list, new_plu_data_list=plu_data,
                                                      articul_dict=articuls_data, used_plus=self.used_plus)
                combined_string_data = "\n".join(combined_plu_data)
                write_log_file(f"{len(combined_plu_data)} PLUs was added into '{plu_path}'. Old PLU file wasn't uploaded to the scale. Number of new PLUs is {new_item_q}")
                self.save_string_to_file(combined_string_data, plu_path)

        if last_changes:
            self.last_change_dict["items"] = last_changes[0]
            self.last_change_dict["prices"] = last_changes[1]

        return True


def main():
    save_data = SaveDataToTXT()
    save_data.connect_fdb()
    while True:
        if not save_data.connection_status:
            save_data.connect_fdb()
        cash_status = save_data.check_cash_status()
        if cash_status == 1:
            save_data.save_to_txt()

        time.sleep(check_time)

if __name__ == "__main__":
    main()
