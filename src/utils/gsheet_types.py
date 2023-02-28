from datetime import datetime


def datetime_to_excel_date(datetime_object, from_datetime=True):
    if datetime_object is None:
        return None
    try:
        secs = datetime.timestamp(datetime_object) if from_datetime else int(datetime_object)
    except OSError:
        secs = datetime_object.timestamp()
    diffpy_ex_h = 25569
    diffpy_ex_s = (diffpy_ex_h * 24) * 60 * 60
    day_ex = (secs + diffpy_ex_s) / (60 * 60 * 24)
    return round(day_ex)
