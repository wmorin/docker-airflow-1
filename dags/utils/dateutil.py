from pytz import utc
from datetime import datetime, timedelta


def NDaysWRTToday(Ndays=0):
    """

    :param Ndays: int
    :return: datetime object representing N days before or after today
    """
    return (datetime.now()+timedelta(days=int(Ndays))).astimezone(utc)


def dateToStr(date, format="%Y-%m-%d %H:%M:%S"):
    """

    :param date: datetime object
    :param format:  string
    :return: string representing the date in the given format
    """
    return date.strftime(format)
