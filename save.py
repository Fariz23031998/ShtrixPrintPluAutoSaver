import fdb
from datetime import datetime
import time
import os

from helper import configure_settings, write_log_file, get_units_type, extract_ip_addresses_from_ini_and_create_path, \
    combine_plu_lists, find_missing_plu_numbers, create_arg_query, get_short_path_name, save_readme_if_not_exists

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

class SaveDataToTXT:
    def __init__(self):
        self.fdb_conn = None
        self.last_sync = 0
        self.path = get_short_path_name(database)
        self.connection_status = False
        self.last_change_dict = {}
        self.last_changes_timestamp = 0
        save_readme_if_not_exists()

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
        SELECT 
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
            # for item in data:
            #     print(item)

        except Exception as e:
            write_log_file(f"Error: {e}")
            self.connection_status = False
            return False
        else:
            return data

    def save_string_to_file(self, text, file_path):
        with open(file_path, 'w', encoding='windows-1251', errors="replace") as file:
            file.write(text)


    def save_to_txt(self):
        last_changes = self.check_last_changes()
        if not last_changes:
            write_log_file(f"DB wasn't changed")
            return False

        data = self.fetch_items()
        if not data:
            write_log_file("No items to save")
            return False

        new_plu_list = []
        added_plu_list = []
        items_without_articul = []

        # Process items with valid articul first
        if use_articul:
            for item in data:
                if item[2] and item[2].isdigit():
                    articul = int(item[2])
                    if articul not in added_plu_list and articul < 23000:
                        unit_type = units_dict.get(item[4])
                        new_plu_list.append(
                            f"{articul};{item[3]};;{item[6] / divider_price};0;0;0;{item[1]};0;0;;01.01.01;{unit_type}")
                        added_plu_list.append(articul)
                    else:
                        items_without_articul.append(item)
                else:
                    items_without_articul.append(item)


        # Process remaining items
        configured_data = items_without_articul if use_articul else data
        plu_numbers = find_missing_plu_numbers(added_plu_list, len(configured_data))

        for index, item in enumerate(configured_data):
            if index <= 22700:
                unit_type = units_dict.get(item[4])  # Default empty if key not found
                new_plu_list.append(
                    f"{plu_numbers[index]};{item[3]};;{item[6] / divider_price};0;0;0;{item[1]};0;0;;01.01.01;{unit_type}")


        string_data = "\n".join(new_plu_list)
        plu_files_path = extract_ip_addresses_from_ini_and_create_path(
            ini_file_path=scales_config_path,
            plu_file_path=plu_file_path
        )

        for plu_path in plu_files_path:
            if not os.path.exists(plu_path):
                write_log_file(f"{len(new_plu_list)} plu's was saved into '{plu_path}'")
                self.save_string_to_file(string_data, plu_path)
            else:
                with open(plu_path, 'r', encoding='windows-1251') as plu_file:
                    old_plu_list = plu_file.readlines()

                combined_plu_data = combine_plu_lists(old_plu_data_list=old_plu_list, new_plu_data_list=new_plu_list)
                combined_string_data = "\n".join(combined_plu_data)
                write_log_file(f"{len(new_plu_list)} plu's was saved into '{plu_path}'")
                self.save_string_to_file(combined_string_data, plu_path)

        if last_changes:
            self.last_change_dict["items"] = last_changes[0]
            self.last_change_dict["prices"] = last_changes[1]

        return True

# save_data = SaveDataToTXT()
# save_data.connect_fdb()
# save_data.save_to_txt()

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
