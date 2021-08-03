from io import BytesIO
from enum import Enum
from datetime import datetime, date
import os

from posixpath import join
import pycurl
import pandas as pd

from custom_reports import CustomReports
from egr_data_process import EgrDataProcess


class ErgMethod(Enum):
    SHORT_INFO = "getShortInfoByPeriod"
    ADDRESS = "getAddressByPeriod"
    EVENTS = "getEventByPeriod"
    BASE_INFO = "getBaseInfoByPeriod"
    VED = "getVEDByPeriod"


class ErgRunMode(Enum):
    PARTIAL_UPDATE = 1
    FULL_UPDATE = 2
    PROCESS = 3


def download_egr_data(method, start_date, end_date):
    request_url = f'http://egr.gov.by/api/v2/egr/{method}/{start_date}/{end_date}'
    print(request_url)
    b_obj = BytesIO()
    crl = pycurl.Curl()
    crl.setopt(crl.URL, f'http://egr.gov.by/api/v2/egr/{method}/{start_date}/{end_date}')
    crl.setopt(crl.WRITEDATA, b_obj)
    crl.perform()
    crl.close()
    get_body = b_obj.getvalue()
    try:
        out = pd.read_json(get_body)
        return out
    except ValueError:
        print(f'error in {request_url}')




def format_date(date_for_format):
    format_pattern = '%d.%m.%Y'
    return date_for_format.strftime(format_pattern)


class ErgParser:
    MAX_DEPTH = 30
    SHIFT = 1
    DEFAULT = 1

    def __init__(self):
        pass

    def run(self, mode, depth=0):
        if mode == ErgRunMode.PARTIAL_UPDATE:
            self.partial_download(depth)
        elif mode == ErgRunMode.FULL_UPDATE:
            self.full_download()
        elif mode == ErgRunMode.PROCESS:
            self.process_data()
            return
        else:
            return
        self.process_data()

    def full_download(self):
        for method in ErgMethod:
            self.download_chunks(depth=self.MAX_DEPTH, method=method.value)

    def partial_download(self, depth):
        for method in ErgMethod:
            self.download_chunks(depth=depth, method=method.value)

    @staticmethod
    def generate_dates(depth):

        today = datetime.today()
        current_year = today.year
        prev = format_date(date(year=current_year - depth - ErgParser.SHIFT,
                                month=ErgParser.DEFAULT, day=ErgParser.DEFAULT))
        for i in range(current_year - depth, today.year + ErgParser.SHIFT):
            d = format_date(date(year=i, month=ErgParser.DEFAULT, day=ErgParser.DEFAULT))
            yield prev, d
            prev = d
        yield d, format_date(date(year=current_year + 1, month=ErgParser.DEFAULT, day=ErgParser.DEFAULT))

    def download_chunks(self, depth, method):
        for start_date, end_date in self.generate_dates(depth):
            data_chunk = download_egr_data(method, start_date, end_date)
            if data_chunk:
                path = join(os.path.dirname(__file__), "data", method, f'{method}_{str(start_date)}_{str(end_date)}.csv')
                data_chunk.to_csv(path, index=False)

    @staticmethod
    def process_data():
        processor = EgrDataProcess()
        processor.process_adresses()
        processor.process_events()
        processor.process_ved()
        processor.process_base_info()
        processor.process_short_info()


if __name__ == "__main__":
    parser = ErgParser()
    parser.run(ErgRunMode.PROCESS)
    CustomReports().update_reports()

