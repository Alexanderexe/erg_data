import pandas as pd
from egr_data_process import EgrDataProcess
from posixpath import join


class CustomReports:
    CUSTOM_REPORTS_DIR = "custom_reports"

    def __init__(self, data_directory, model_dir_name):
        self.processor = EgrDataProcess(root_dir=data_directory, egr_model=model_dir_name)

    def update_reports(self):
        self.get_new_clients()
        self.get_active_orgs_info()

    def get_active_orgs_info(self):
        base_info = pd.read_csv(self.processor.base_model)
        short_info = pd.read_csv(self.processor.short_info_model)
        oked_info = pd.read_csv(self.processor.ved_model)
        df = (base_info[base_info['status'] == 'Действующий'].merge(oked_info[oked_info.dto.isna()], on='ngrn')[
             ['ngrn', 'dfrom_x', 'vnuzp', 'ved_name', 'ved_code']]
         .merge(short_info, on='ngrn'))
        df.dfrom_x = pd.to_datetime(df.dfrom_x)
        df.dfrom_x = df.dfrom_x.dt.date
        compression_options = dict(method='zip', archive_name=f'active_info.csv')
        df.to_csv(join(self.CUSTOM_REPORTS_DIR, "active_info.zip"), index=False, compression=compression_options)

    def get_new_clients(self, gap=6):
        interval_start = (pd.Timestamp.utcnow() - pd.Timedelta(days=gap)).ceil('D')

        base_info = pd.read_csv(self.processor.base_model)
        adresses = pd.read_csv(self.processor.adr_model)
        short_info = pd.read_csv(self.processor.short_info_model)

        base_info.dfrom = pd.to_datetime(base_info.dfrom)
        adresses.dfrom = pd.to_datetime(adresses.dfrom)

        new_active_orgs = base_info[base_info.dfrom >= interval_start]
        new_adresses = adresses[adresses.dfrom >= interval_start]
        print(new_adresses.dfrom.max())
        out = new_adresses.merge(new_active_orgs, on='ngrn').merge(short_info, on='ngrn')
        out.dfrom_x = out.dfrom_x.dt.date
        out.dfrom_y = out.dfrom_y.dt.date
        out.to_excel(join(self.CUSTOM_REPORTS_DIR, "new_clients.xlsx"), index=False)
        print('max_date', out.dfrom_y.max())
