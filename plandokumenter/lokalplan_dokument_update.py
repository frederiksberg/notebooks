import psycopg2
import requests
from sqlalchemy import create_engine, Table, MetaData
import sys
import io
import logging
import textract
import PyPDF2
import os
sys.path.append('/Users/andersbarfod/Documents/python/')
import connections as con

logging.basicConfig(filename='logfile.log', filemode='w')

def download_pdf(url, folder):
    """
    Download pdf from url to folder
    """
    try:
        r = requests.get(url)
    except Exception as e:
        print(e)
        logging.warning(e)
    pdf_name = url.split('/')[-1]
    path = folder + pdf_name
    with open(path, 'wb') as file:
        file.write(r.content)
    return pdf_name

def get_document(url, temp_folder='./', language='dan'):
    """
    Get pdf documents from URL and extract the text. 
    If the pdf doesn't contain text textract is used to 
    OCR scan the document, which entails temporary downloads of pdf's.
    The function uses logging of exceptions, so make sure to setop logging basic config.
    
    logging.basicConfig(filename='logfile.log', filemode='w')
    """
    try:
        r = requests.get(url)
    except Exception as e:
        print(e)
        logging.warning(e)
    # Convert to in-memory binary streams and read pdf
    pdf_file = io.BytesIO(r.content)
    pdfReader = PyPDF2.PdfFileReader(pdf_file, strict=False)

    # Discerning the number of pages will allow us to parse through all the pages
    num_pages = pdfReader.numPages
    count = 0
    text = ""
    # The while loop will read each page
    while count < num_pages:
        try:
            pageObj = pdfReader.getPage(count)
            count +=1
            text += pageObj.extractText()
        except Exception as e:
            logging.warning(e)
            print(e, url)
    #If the below returns as False, we run the OCR library textract to 
    # convert scanned/image based PDF files into text         
    if text != "":
        text = text
    else:
        try:
            # Download pdf to disk in order to use textract (might be possible to do in-memory)
            filename = download_pdf(url, temp_folder)
            text = textract.process(temp_folder + filename, method='tesseract', language=language)
            text = text.decode('utf-8')
            if os.path.exists(filename):
                os.remove(filename)
        except Exception as e:
            logging.warning(e)
            print(e, url)

    return text

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
                document = get_document(r['doklink'])
            )
            plan_insert = plan.insert().values(val)
            engine.execute(plan_insert)
            logging.info('Inserted: %s', plan.c.planid)
        elif r['handling'] == 'UPDATE':
            val = dict(
                plannavn = r['plannavn'],
                document = get_document(r['doklink']), 
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