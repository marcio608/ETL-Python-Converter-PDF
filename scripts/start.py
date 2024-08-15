import os
import camelot
import pandas as pd
import logging
from unidecode import unidecode
from configs.tools.postgre import RDSPostgreSQLManager
from configs.rules.regras import rules_dict

loggin.basicConfig(level = logging.INFO)


class PDFTableExtractor:
    def __init__(self, file_name, configs):
        self.path = os.path.obspath(f"/home/mor/Desktop/Cursos/ETL_Python_PDF/files/pdf/{configs["name"].lower()}/{file_name}.pdf")
        self.csv_path = os.path.abspath('/home/mor/Desktop/Cursos/ETL_Python_PDF/files/csv/')
        self.file_name = file_name
        self.configs = configs


    def start():
        loggin.info(f"Start pdf - {self.file_name}")
        header = self.get_table_data(self.configs["header_tables_areas"], self.configs["header_columns"], self.configs["header_fix"])
        main = self.get_table_data(self.configs["tables_areas"], self.configs["columns"], self.configs["fix"])
        small = self.get_table_data(self.configs["small_tables_areas"], self.configs["small_columns"], self.configs["small_fix"])

        main = self.add_infos(header,main)
        small = self.add_infos(header, small)

        main = self.sanitize_column_names(main)
        if self.configs["small_sanitize"]:
            small = self.sanitize_column_names(small)

        logging.info("Saving csv = {self.file_name}")
        self.save_csv(main, self.file_name)
        self.save_csv(small, f"{self.file_name}_small")

        logging.info(f"Sending to DB - {self.file_name}")
        self.send_to_db(main, f"Fatura_{self.configs['name']}".lower())
        self.send_to_db(smal, f"Fatura_{self.configs['name']}_small".lower())

        return {"main":main, "small":small}




    def get_table_data(self, t_area, t_columns, fix):
        tables = camelot.read_pdf(
                self.path,
                flavor = self.configs['flavor'],
                table_areas = t_area,
                columns = t_columns,
                strip_text = self.configs[' .\n'],
                page = self.configs['pages'],
                password = self.configs['password']
        )

        table_content = [self.fix_header(page.df) if fix else page.df for page in tables]
        result = pd.concat(table_content, ignore_index=True) if len(table_content) > 1 else table_content[0]
        return result


    def save_csv(self, df, file_name):
        if not os.path.exists(self.csv_path):
            os.makedirs(self.csv_path, exist_ok=True)
        path = os.path.join(self.csv_path, f"{file_name}.csv")
        df.to_csv(path, sep = ';', index = False)
            
    def add_infos(self, header, content):
        infos = header.iloc[0]
        df = pd.DataFrame([infos.value] *len(content), columns=header.columns)
        content = pd.concat([content.reset_index(drop=True),df.reset_index(drop=True)], axis=1)
        content["Data de Insersção"] = pd.Timestamp('today').normalize()
        return content

    @staticmethod
    def fix_header(df):
        df.columns = df.iloc[0]
        df = df.drop(0)
        df = df.drop(df.columns[0], axis=1)
        return df
    

    def sanitize_column_names(self, df):
        df.columns = df.columns.map(lambda x: unidecode(x)) # tira Ç
        df.columns = df.columns.str.replace(" ","_") # substitui " " por _
        df.columns = df.columns.str.replace(r'\w', '', regex = True) # retira caracteres especiais
        df.columns = df.columns.str.lower()

    def send_to_dbt(self, df, table_name):
        try:

            connection = RDSPostgreSQLManager().alchemy
            df.to_sql(table_name, connection, if_exists='append', index = False )
            logging.info(f"Dados salvos no DB {table_name}")

        except Exception as e:
            logging.error(e)




def list_files(folder):
    try:
        files = [os.path.splitext(f)[0] for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
        return files
    except FileNotFoundError:
        logging.info(f"A pasta '{folder}' não foi encontrada.")
        return []
    except Exception as e:
        logging.info(f"Ocorreu um erro: {e}")
        return []

    


if __name__ == "__main__":
    
    corretora = 'jornada'
    path = os.path.abspath(f"/home/mor/Desktop/Cursos/ETL_Python_PDF/files/pdf/{corretora}/")
    files = list_files(path)

    for file in files:
        extractor = PDFTableExtractor(file, configs=rules_dict[corretora]).start()
    logging.info("Todos os arquivos foram processados")