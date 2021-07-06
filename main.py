from io import BytesIO
from enum import Enum
from datetime import datetime

import pycurl
import pandas as pd


class ErgMethod(Enum):
    ADDRESS = "getAddressByPeriod"
    EVENTS = "getEventByPeriod"
    BASE_INFO = "getBaseInfoByPeriod"
    SHORT_INFO = "getShortInfoByPeriod"
    VED = "getVEDByPeriod"


def download_egr_data(method, start_date, end_date):
    b_obj = BytesIO()
    crl = pycurl.Curl()
    crl.setopt(crl.URL, f'http://egr.gov.by/api/v2/egr/{method}/{start_date}/{end_date}')
    crl.setopt(crl.WRITEDATA, b_obj)
    crl.perform()
    crl.close()
    get_body = b_obj.getvalue()
    out = pd.read_json(get_body)
    return out


class ErgParser:
    def __init__(self):
        self.today = datetime.today()

    def full_download(self):
        pass

    def partial_download(self):
        pass

    def download_addresses(self):
        pass
