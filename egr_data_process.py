import os

import pandas as pd
from posixpath import join
import json


def get_value(data, value):
    if type(data) == str:
        tmp = data.replace("'", '"')
        data = json.loads(tmp)
        return data[value]


def get_value_eval(data, value):
    if type(data) == str:
        data = eval(data)
        return data[value]


class EgrDataProcess:
    ROOT_DIR = "DATA"
    ADRESSES_DIR = join(ROOT_DIR, "getAddressByPeriod")
    EVENTS_DIR = join(ROOT_DIR, "getEventByPeriod")
    SHORT_INFO = join(ROOT_DIR, "getShortInfoByPeriod")
    BASE_INFO = join(ROOT_DIR, "getBaseInfoByPeriod")
    VED_DIR = join(ROOT_DIR, "getVEDByPeriod")
    MODEL_DIR = join(ROOT_DIR, "erg_model")
    ADRESSES_MODEL = join(MODEL_DIR, "adresses.csv")
    EVENTS_MODEL = join(MODEL_DIR, "events.csv")
    SHORT_INFO_MODEL = join(MODEL_DIR, "short_info.csv")
    BASE_MODEL = join(MODEL_DIR, "base_info.csv")
    VED_MODEL = join(MODEL_DIR, "ved.csv")

    @staticmethod
    def concat_files(directory):
        df = pd.DataFrame()
        for file in os.listdir(directory):
            df = df.append(pd.read_csv(os.path.join(directory, file)))
        return df

    def process_adresses(self):
        adresses = self.concat_files(self.ADRESSES_DIR)
        adresses = adresses[
            ['ngrn', 'dfrom', 'dto', 'nindex', 'nsi00202', 'vregion', 'vdistrict', 'vnp', 'vtels', 'vemail', 'vsite',
             'vfax']]
        adresses.loc[:, 'vnsfull'] = adresses.loc[:, 'nsi00202'].apply(lambda x: get_value(x, 'vnsfull'))
        adresses = adresses.drop(columns=[i for i in adresses.columns if 'nsi' in i])
        adresses.to_csv(self.ADRESSES_MODEL, index=False)

    def process_events(self):
        events = self.concat_files(self.EVENTS_DIR)
        events = events[
            ['ngrn', 'ddoc', 'vdocn', 'dsrok', 'dfrom', 'dto', 'nsi00223', 'nsi00212R', 'nsi00213', 'nsi00212',
             'vprim']]
        events.loc[:, 'decision_code'] = events.loc[:, 'nsi00223'].apply(lambda x: get_value(x, 'vnop'))
        events.loc[:, 'decision_name'] = events.loc[:, 'nsi00223'].apply(lambda x: get_value(x, 'nsi00223'))
        events.loc[:, 'decision_nkuz'] = events.loc[:, 'nsi00212R'].apply(lambda x: get_value_eval(x, 'nkuz'))
        events.loc[:, 'basis_code'] = events.loc[:, 'nsi00213'].apply(lambda x: get_value(x, 'nkosn'))
        events.loc[:, 'basis_name'] = events.loc[:, 'nsi00213'].apply(lambda x: get_value(x, 'vnosn'))
        events.loc[:, 'doc_nkuz'] = events.loc[:, 'nsi00212'].apply(lambda x: get_value_eval(x, 'nkuz'))
        events = events.drop(columns=[i for i in events.columns if 'nsi' in i])
        events.to_csv(self.EVENTS_MODEL, index=False)

    def process_base_info(self):
        base_info = self.concat_files(self.BASE_INFO)
        base_info = base_info[['ngrn', 'dfrom', 'dto', 'nsi00219', 'nsi00211', 'nsi00212']]
        base_info.loc[:, 'status'] = base_info.loc[:, 'nsi00219'].apply(lambda x: get_value(x, 'vnsostk'))
        base_info.loc[:, 'type'] = base_info.loc[:, 'nsi00211'].apply(lambda x: get_value(x, 'nkvob'))
        base_info.loc[:, 'vnuzp'] = base_info.loc[:, 'nsi00212'].apply(lambda x: get_value_eval(x, 'vnuzp'))
        base_info = base_info.drop(columns=[i for i in base_info.columns if 'nsi' in i])
        base_info.to_csv(self.BASE_MODEL, index=False)

    def process_short_info(self):
        short_info = self.concat_files(self.SHORT_INFO)
        short_info = short_info[['vfio', 'ngrn', 'vnaim']]
        short_info.to_csv(self.SHORT_INFO_MODEL, index=False)

    def process_ved(self):
        ved = self.concat_files(self.VED_DIR)
        ved.loc[:, 'ved_name'] = ved.loc[:, 'nsi00114'].apply(lambda x: get_value(x, 'vnvdnp'))
        ved.loc[:, 'ved_code'] = ved.loc[:, 'nsi00114'].apply(lambda x: get_value(x, 'vkvdn'))
        ved = ved.drop(columns=[i for i in ved.columns if 'nsi' in i])
        ved.to_csv(self.VED_MODEL, index=False)
