from io import BytesIO
from enum import Enum
from datetime import datetime, date
import os
import logging as l
import random
import argparse

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
    PARTIAL_UPDATE = 'PARTIAL'
    FULL_UPDATE = 'FULL'
    PROCESS = 'PROCESS'
    SKIP = 'SKIP'


class Session():
    def __init__(self, root_dir, model_dir, mode):
        self.id = None
        self.directory = None
        self.logger = None
        self.parser = None
        self.root_dir = root_dir
        self.model_dir = model_dir
        self.mode = mode

    def create_session(self):
        self.id = random.getrandbits(16)
        self.directory = join(self.root_dir, f'{self.mode}' \
                                             f'-{datetime.today().strftime(format="%d-%m-%Y__%H-%M")}')
        self.create_directories_structure()

        self.logger = l.Logger('logger')
        fh = l.FileHandler(f'{self.directory}/log.txt')
        fh.setLevel(l.INFO)
        ch = l.StreamHandler()
        ch.setLevel(l.INFO)
        # create formatter and add it to the handlers
        formatter = l.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        self.logger.addHandler(fh)
        self.logger.info(f'session {self.id} is created at {datetime.today()} with {self.directory} path')

    def start_session(self):
        self.logger.info(f'run parsing with {self.mode} mode')
        self.parser = ErgParser(self.logger, self.directory, self.model_dir)
        self.parser.run(mode=self.mode)

    def create_directories_structure(self):
        os.mkdir(self.directory)
        for method_name in ErgMethod:
            os.mkdir(join(self.directory, method_name.value))
        os.mkdir(join(self.directory, self.model_dir))


def download_egr_data(method, start_date, end_date, logger):
    request_url = f'http://egr.gov.by/api/v2/egr/{method}/{start_date}/{end_date}'
    logger.info(f'start download {request_url}')
    b_obj = BytesIO()
    crl = pycurl.Curl()
    crl.setopt(crl.URL, request_url)
    crl.setopt(crl.WRITEDATA, b_obj)
    crl.perform()
    crl.close()
    get_body = b_obj.getvalue()
    try:
        out = pd.read_json(get_body)
        logger.info(f'successful read downloaded json {request_url}')
        return out
    except ValueError:
        raise ValueError
        logger.info(f'pandas cant read downloaded json {request_url}')


def format_date(date_for_format):
    format_pattern = '%d.%m.%Y'
    return date_for_format.strftime(format_pattern)


class ErgParser:
    MAX_DEPTH = 30
    SHIFT = 1
    DEFAULT = 1

    def __init__(self, logger, data_directory, model_dir_name):
        self.processor = EgrDataProcess(data_directory, logger=logger)
        self.logger = logger
        self.data_directory = data_directory

    def run(self, mode, depth=0):
        if mode == ErgRunMode.PARTIAL_UPDATE:
            self.partial_download(depth)
        elif mode == ErgRunMode.FULL_UPDATE:
            self.full_download()
        elif mode == ErgRunMode.PROCESS:
            self.process_data()
            return
        elif mode == ErgRunMode.SKIP:
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
            data_chunk = download_egr_data(method, start_date, end_date, self.logger)
            if not data_chunk.empty:
                path = join(os.path.dirname(__file__), self.data_directory, method,
                            f'{method}_{str(start_date)}_{str(end_date)}.csv')
                data_chunk.to_csv(path, index=False)
                self.logger.info(f'chunk save to {path}')

    def process_data(self):
        self.processor.process_adresses()
        self.processor.process_events()
        self.processor.process_ved()
        self.processor.process_base_info()
        self.processor.process_short_info()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_folder', '-d', type=str, help='path to data_folder', required=True,)
    parser.add_argument('--mode', '-m', required=True,
                        help=f'int mode {[i for i in ErgRunMode]}')
    parser.add_argument('--model_folder', '-mf', type=str, help='path_to_save_model_dir',required=True,)

    args = parser.parse_args()
    session = Session('data', 'egr_model', ErgRunMode[args.mode.upper()])
    session.create_session()
    session.start_session()
