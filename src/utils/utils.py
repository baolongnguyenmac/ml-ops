from datetime import datetime

def check_date(date_str:str) -> bool:
    # check if the string is valid by converting to time
    try:
        datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        return True
    except ValueError as e:
        print(f'[Ingest bronze] Expect string in timestamp but receive {date_str} - {e}')
        return False
