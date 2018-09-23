import math
from datetime import datetime,date, time
from .settings import *

DISPLAY_ROWS = 20
DISPLAY_COLS = 100
DISPLAY_WIDTH = 100

cumMonthDays = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365]
cumLeapMonthDays = [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366]
monthDays = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
leapMonthDays = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
START_DATE = date(2000, 0o01, 0o01)

class temporal(object):
    def __init__(self, val):
        """xxdb int/long representation of temporal objects"""
        self.__value = val

    @property
    def value(self):
        return self.__value

    # def __eq__(cls, self, other):
    #     """Override the default Equals behavior"""
    #     if isinstance(other, self.__class__): return self.__value == other.__value
    #     if isinstance(other, int) or isinstance(other, long): return self.__value == other
    #     return False


class Date(temporal):

    @classmethod
    def from_date(cls, date):
        """create a Date instance given datetime.date object"""
        return cls(countDays(date))

    @classmethod
    def null(cls):
        return cls(DBNAN[DT_DATE])

    @classmethod
    def isnull(cls, obj):
        return obj.value == DBNAN[DT_DATE]

    def to_date(self):
        return parseDate(self.value)

    def __repr__(self):
        """"convert days into datetime.date object first """
        if self.value == DBNAN[DT_DATE]: return ''
        date = parseDate(self.value)
        return "{0:04d}.{1:02d}.{2:02d}".format(date.year, date.month, date.day)


class Month(temporal):

    @classmethod
    def from_date(cls, date):
        """create a Month instance given datetime.date object"""
        return cls(date.year * 12 + date.month - 1)

    @classmethod
    def null(cls):
        return cls(DBNAN[DT_MONTH])

    @classmethod
    def isnull(cls, obj):
        return obj.value == DBNAN[DT_MONTH]

    def to_date(self):
        return date(int(self.value/12), int(self.value % 12) + 1, 1)

    def __repr__(self):
        if self.value == DBNAN[DT_MONTH]: return ''
        return "{0:04d}.{1:02d}M".format(int(self.value/12), int(self.value % 12 + 1))


class Time(temporal):

    @classmethod
    def from_time(cls, time):
        """create a time instance given datetime.time object"""
        return cls(((time.hour * 60 + time.minute) * 60 + time.second) * 1000 + int(time.microsecond/1000))

    @classmethod
    def null(cls):
        return cls(DBNAN[DT_TIME])

    @classmethod
    def isnull(cls, obj):
        return obj.value == DBNAN[DT_TIME]

    def to_time(self):
        return time(int(self.value / 3600000), int(self.value / 60000 % 60), int(self.value / 1000 % 60), int(self.value % 1000 * 1000))

    def __repr__(self):
        if self.value == DBNAN[DT_TIME]: return ''
        return "{0:02d}:{1:02d}:{2:02d}.{3:03d}".format(int(self.value / 3600000), int(self.value / 60000 % 60), int(self.value / 1000 % 60), int(self.value % 1000))


class Minute(temporal):

    @classmethod
    def from_time(cls, time):
        """create a time instance given datetime.time object"""
        return cls(time.hour * 60 + time.minute)

    @classmethod
    def null(cls, obj):
        return cls(DBNAN[DT_TIME])

    @classmethod
    def isnull(cls, obj):
        return obj.value == DBNAN[DT_TIME]

    def to_time(self):
        return time(int(self.value / 60), int(self.value % 60))

    def __repr__(self):
        if self.value == DBNAN[DT_TIME]: return ''
        return "{0:02d}:{1:02d}m".format( int(self.value / 60), int(self.value % 60))


class Second(temporal):

    @classmethod
    def from_time(cls, time):
        """create a second instance given datetime.time object"""
        return cls((time.hour * 60 + time.minute) * 60 + time.second)

    @classmethod
    def null(cls):
        return cls(DBNAN[DT_SECOND])

    @classmethod
    def isnull(cls, obj):
        return obj.value == DBNAN[DT_SECOND]

    def to_time(self):
        return time(int(self.value / 3600), int(self.value % 3600 / 60), int(self.value % 60))

    def __repr__(self):
        if self.value == DBNAN[DT_SECOND]: return ''
        return "{0:02d}:{1:02d}:{2:02d}".format( int(self.value / 3600), int(self.value % 3600 / 60), int(self.value % 60))


class Datetime(temporal):

    @classmethod
    def from_datetime(cls, date_time):
        """create a Datetime instance given datetime object"""
        return cls(countDateTimeSeconds(date_time))

    @classmethod
    def null(cls):
        return cls(DBNAN[DT_DATETIME])

    @classmethod
    def isnull(cls, obj):
        return obj.value == DBNAN[DT_DATETIME]

    def to_datetime(self):
        return parseDateTime(self.value)

    def __repr__(self):
        if self.value == DBNAN[DT_DATETIME]: return ''
        dt = self.to_datetime()
        return "{0:04d}.{1:02d}.{2:02d}T{3:02d}:{4:02d}:{5:02d}".format(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)


class Timestamp(temporal):
    @classmethod
    def from_datetime(cls, date_time):
        """create a TimeStamp instance given datetime object"""
        return cls(countMilliseconds(date_time))

    @classmethod
    def null(cls):
        return cls(DBNAN[DT_TIMESTAMP])

    @classmethod
    def isnull(cls, obj):
        return obj.value == DBNAN[DT_TIMESTAMP]

    def to_datetime(self):
        return parseTimestamp(self.value)

    def __repr__(self):
        if self.value == DBNAN[DT_TIMESTAMP]: return ''
        dt = self.to_datetime()
        return "{0:04d}.{1:02d}.{2:02d}T{3:02d}:{4:02d}:{5:02d}.{6:03d}".format(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond)


class NanoTime(temporal):
    @classmethod
    def from_time(cls, time):
        return cls(countNanotime(time))

    @classmethod
    def null(cls):
        return cls(DBNAN[DT_NANOTIME])

    @classmethod
    def isnull(cls, obj):
        return obj.value == DBNAN[DT_NANOTIME]

    def to_nanotime(self):
        mili = int(self.value / 1000000)
        return time(int(mili / 3600000),int(mili / 60000 % 60), int(mili / 1000 % 60), int((self.value % 1000000000)/1000)) #??

    def __repr__(self):
        if self.value == DBNAN[DT_NANOTIME]: return ''
        mili = int(self.value / 1000000)
        return "{0:02d}:{1:02d}:{2:02d}.{3:03d}".format(int(mili / 3600000), int(mili / 60000 % 60), int(mili / 1000 % 60), int(self.value % 1000000000))



class NanoTimestamp(temporal):
    @classmethod
    def from_datetime(cls, date_time):
        """create a TimeStamp instance given datetime object"""
        return cls(countNanoseconds(date_time))

    @classmethod
    def null(cls):
        return cls(DBNAN[DT_NANOTIMESTAMP])

    @classmethod
    def isnull(cls, obj):
        return obj.value == DBNAN[DT_NANOTIMESTAMP]

    def to_nanotimestamp(self):
        return parseNanoTimestamp(self.value) #??

    def __repr__(self):
        if self.value == DBNAN[DT_NANOTIMESTAMP]: return ''
        dt = parseNanoTimestamp(self.value)
        return "{0:04d}.{1:02d}.{2:02d}T{3:02d}:{4:02d}:{5:02d}.{6:09d}".format(dt.year, dt.month, dt.day, dt.hour,dt.minute, dt.second, self.value)


def countDays(date):
    day = date.day
    month = date.month
    year = date.year

    divide400Years = int(year / 400)
    offset400Years = int(year % 400)
    days = divide400Years * 146097 + offset400Years * 365 - 719529;
    if offset400Years > 0:
        days += int((offset400Years - 1) / 4) + 1 - int((offset400Years - 1) / 100)
    if (year % 4 == 0 and year % 100 != 0) or year % 400 == 0:
        days += cumLeapMonthDays[month-1];
        days += day if day <= leapMonthDays[month - 1] else 0;
    else:
        days += cumMonthDays[month - 1]
        days += day if day <= monthDays[month - 1]  else 0
    return days

def parseDate(days):

    days += 719529

    circleIn400Years = int(days / 146097)
    offsetIn400Years = int(days % 146097)

    resultYear = circleIn400Years * 400
    similarYears = int(offsetIn400Years / 365)

    tmpDays = similarYears * 365
    if similarYears > 0:
        tmp = int((similarYears - 1) / 4) + 1 - int((similarYears - 1) / 100)
        if tmp%1 > 0.72:
            tmp = int(tmp)+1
        tmpDays += tmp
    if (tmpDays >= offsetIn400Years):
        similarYears -= 1

    year = similarYears + resultYear
    year = int(year)

    days -= int(circleIn400Years * 146097 + tmpDays)
    leap = (int(year % 4 == 0) and int(year % 100 != 0)) or int(year % 400) == 0
    if days <= 0:
        days += 366 if leap else 365

    if leap:
        month= int(days / 32+1)
        if days > cumLeapMonthDays[month]:
            month += 1
        day=int(days-cumLeapMonthDays[month-1])
    else:
        month = int(days / 32+1)
        if days > cumMonthDays[month]:
            month += 1
        day=int(days - cumMonthDays[month-1])
    return date(year, month, day)

def countDateTimeSeconds(date_time):
    days = countDays(date_time.date())
    return days * 86400 + (date_time.hour * 60 + date_time.minute) * 60 + date_time.second


def countMilliseconds(date_time):
    return countDateTimeSeconds(date_time) * 1000 + int(date_time.microsecond/1000)

def countNanoseconds(date_time):
    return countDateTimeSeconds(date_time) * 1000000000 + int(date_time.microsecond * 1000)

def countNanotime(time):
    secs = (time.hour * 60 + time.minute) * 60 + time.second
    return secs * 1000000000 + time.microsecond * 1000;

def parseDateTime(seconds):
    days = int(seconds / 86400)
    dt = parseDate(days)
    seconds = int(seconds % 86400)
    if (seconds < 0):
        seconds += 86400
    hour = int(seconds / 3600)
    seconds = int(seconds % 3600)
    minute = int(seconds / 60)
    second = int(seconds % 60)
    return datetime(dt.year, dt.month, dt.day, hour, minute, second)

def parseTimestamp(milliseconds):
    days = int(milliseconds / 86400000)
    dt = parseDate(days)
    milliseconds = int(milliseconds % 86400000)
    if (milliseconds < 0):
        milliseconds += 86400000
    microsecond = int(milliseconds % 1000 * 1000)
    seconds = int(milliseconds / 1000)
    hour = int(seconds / 3600)
    seconds = int(seconds % 3600)
    minute = int(seconds / 60)
    second = int(seconds % 60)
    return datetime(dt.year, dt.month, dt.day, hour, minute, second, microsecond)

def parseNanoTimestamp(nanoseconds):
    days = int(nanoseconds / (86400000 * 1000000))
    dt = parseDate(days)
    miliseconds = (int(nanoseconds/1000000)) % 86400000
    #microsecond = (nanoseconds/1000) % 864000L
    if (miliseconds < 0):
        miliseconds += 86400000
    microsecond = int((nanoseconds % 1000000000)/1000)
    seconds = int(miliseconds / 1000 )
    hour = int(seconds / 3600)
    seconds = int(seconds % 3600)
    minute = int(seconds / 60)
    second = int(seconds % 60)
    return datetime(dt.year, dt.month, dt.day, hour, minute, second, microsecond)