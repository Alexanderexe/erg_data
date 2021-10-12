import logging
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

    def __init__(self, root_dir='DATA', egr_model="egr_model", logger=None):
        self.adresses_dir = join(root_dir, "getAddressByPeriod")
        self.events_dir = join(root_dir, "getEventByPeriod")
        self.short_info = join(root_dir, "getShortInfoByPeriod")
        self.base_info = join(root_dir, "getBaseInfoByPeriod")
        self.ved_dir = join(root_dir, "getVEDByPeriod")
        self.model_dir = join(root_dir, egr_model)
        self.adr_model = join(self.model_dir, "adresses.csv")
        self.event_model = join(self.model_dir, "events.csv")
        self.short_info_model = join(self.model_dir, "short_info.csv")
        self.base_model = join(self.model_dir, "base_info.csv")
        self.ved_model = join(self.model_dir, "ved.csv")
        self.logger = logger

    def concat_files(self, directory):
        df = pd.DataFrame()
        self.logger.info(f'concat {directory} files')
        for file in os.listdir(directory):
            df = df.append(pd.read_csv(os.path.join(directory, file)))
        return df

    def process_adresses(self):
        self.logger.info(f'process adresses data')
        adresses = self.concat_files(self.adresses_dir)
        adresses = adresses[
            ['ngrn', 'dfrom', 'dto', 'nindex', 'nsi00202', 'vregion', 'vdistrict', 'vnp', 'vtels', 'vemail', 'vsite',
             'vfax']]
        adresses.loc[:, 'vnsfull'] = adresses.loc[:, 'nsi00202'].apply(lambda x: get_value(x, 'vnsfull'))
        adresses = adresses.drop(columns=[i for i in adresses.columns if 'nsi' in i])
        adresses.to_csv(self.adr_model, index=False)

    def process_events(self):
        self.logger.info(f'process events data')
        events = self.concat_files(self.events_dir)
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
        events.to_csv(self.event_model, index=False)

    def process_base_info(self):
        self.logger.info(f'process baseinfo data')
        base_info = self.concat_files(self.base_info)
        base_info = base_info[['ngrn', 'dfrom', 'dto', 'nsi00219', 'nsi00211', 'nsi00212']]
        base_info.loc[:, 'status'] = base_info.loc[:, 'nsi00219'].apply(lambda x: get_value(x, 'vnsostk'))
        base_info.loc[:, 'type'] = base_info.loc[:, 'nsi00211'].apply(lambda x: get_value(x, 'nkvob'))
        base_info.loc[:, 'vnuzp'] = base_info.loc[:, 'nsi00212'].apply(lambda x: get_value_eval(x, 'vnuzp'))
        base_info = base_info.drop(columns=[i for i in base_info.columns if 'nsi' in i])
        base_info.to_csv(self.base_model, index=False)

    def process_short_info(self):
        self.logger.info(f'process shortinfo data')
        short_info = self.concat_files(self.short_info)
        short_info = short_info[['vfio', 'ngrn', 'vnaim']]
        short_info.to_csv(self.short_info_model, index=False)

    def process_ved(self):
        self.logger.info(f'process ved data')
        ved = self.concat_files(self.ved_dir)
        ved.loc[:, 'ved_name'] = ved.loc[:, 'nsi00114'].apply(lambda x: get_value(x, 'vnvdnp'))
        ved.loc[:, 'ved_code'] = ved.loc[:, 'nsi00114'].apply(lambda x: get_value(x, 'vkvdn'))
        ved = ved.drop(columns=[i for i in ved.columns if 'nsi' in i])
        ved.to_csv(self.ved_model, index=False)
