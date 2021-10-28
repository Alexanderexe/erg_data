import pandas as pd
from egr_data_process import EgrDataProcess
from posixpath import join


class CustomReports:
    CUSTOM_REPORTS_DIR = "custom_reports"

    def __init__(self, data_folder_name, model_folder):
        self.processor = EgrDataProcess(data_folder_name, model_folder)

    def update_reports(self):
        self.get_new_clients()

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
