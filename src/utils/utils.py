from datetime import datetime

def check_date(date_str:str) -> bool:
    # check if the string is valid by converting to time
    try:
        datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f")
        return True
    except ValueError as e:
        raise f'[check_date] Expect string in timestamp but receive {date_str} - {e}'
