import psycopg2
import requests
from sqlalchemy import create_engine, Table, MetaData
import sys
import io
import logging
import textract
import PyPDF2
import os
import connections as con
from functools import reduce
dir_path = os.path.dirname(os.path.realpath(__file__))

logging.basicConfig(filename='logfile.log', filemode='w')

def download_document(url):
    try:
        r = requests.get(url)
    except Exception as e:
        print(e)

    pdf_name = url.split('/')[-1]
    path = dir_path + pdf_name
    with open(path, 'wb') as file:
        file.write(r.content)
    return path


def process_document(url):

    pdf_path = download_document(url)

    # Convert to in-memory binary streams and read pdf
    with open(f'{pdf_path}.pdf', "rb") as file:
        pdfReader = PyPDF2.PdfFileReader(file,strict = True)

        pdf_pages = map(lambda x: pdfReader.getPage(x).extractText(),range(pdfReader.numPages))
        pdf_text = reduce(lambda a,b: a + b, pdf_pages,'')

        return pdf_text if pdf_text else textract_document(pdf_path)
    
    os.remove(f'{pdf_path}.pdf')

       

def textract_document(pdf_path,language='dan'):
    pdf_text = textract.process(f'{pdf_path}', method='tesseract', language=language)
    return pdf_text.decode('utf-8')

def update_table(plan):
    sql = '''
        select *
        FROM elasticsearch.lokalplan_dokument_update
        '''
    result_set = connection.execute(sql)  
    for r in result_set:
        if r['handling'] == 'INSERT':
            val = dict(
                planid = r['planid'],
                plannavn = r['plannavn'],
                status = r['ny_status'],
                document = process_document(r['doklink'])
            )
            plan_insert = plan.insert().values(val)
            engine.execute(plan_insert)
            logging.info('Inserted: %s', plan.c.planid)
        elif r['handling'] == 'UPDATE':
            val = dict(
                plannavn = r['plannavn'],
                document = process_document(r['doklink']), 
                status = r['ny_status']
            )
            plan_update = plan.update().values(val).where(plan.c.planid == r['planid'])
            engine.execute(plan_update)
            logging.info('Updated: %s', plan.c.planid)
        elif r['handling'] == 'DELETE':
            plan_delete = plan.delete().where(plan.c.planid == r['planid'])
            engine.execute(plan_delete)
            logging.info('Deleted: %s', plan.c.planid)
        else:
            logging.warning('Ukendt CRUD handling')

    connection.close()



if __name__ == "__main__":
    engine = con.engine('production')

    connection = engine.connect()
    metadata = MetaData()
    plan = Table('lokalplan_dokument', metadata, autoload=True, autoload_with=engine, schema='proj_anba14')
    update_table(plan)