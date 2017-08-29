import luigi

import os

from EE.engine_from_json_file import engine_from_json_file

from EE.e1 import *
from EE.e2 import *


class ImportData(luigi.Task) :
    dir = luigi.Parameter(default='.')
    date = luigi.DateParameter(default=datetime.date.today())
    sql_json=luigi.Parameter(significant=False,default =os.getenv('SQL_JSON'))


    def output(self):
        output_path = 'Android_{}.csv'.format(self.date)
        return luigi.LocalTarget(os.path.join(str(self.dir), output_path))

    def run (self) :
        engine=engine_from_json_file(str(self.sql_json))
        imp(con=engine,output_path=self.output().path)

class PngFig(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())
    sql_json = luigi.Parameter(significant=False, default=os.getenv('SQL_JSON'))
    data_dir = luigi.Parameter(default='.')
    png_dir=luigi.Parameter(default='.')


    def requires(self):
        return ImportData(sql_json=self.sql_json ,dir=self.data_dir)

    def output(self):
        file_name='Android_{}.png'.format(self.date)
        return luigi.LocalTarget(os.path.join(str(self.data_dir), file_name))

    def run(self):
        csv_to_png(self.input().path, self.data_dir)








