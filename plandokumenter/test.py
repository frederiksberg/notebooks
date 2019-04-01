import PyPDF2
import requests
import io
import os
from functools import reduce
dir_path = os.path.dirname(os.path.realpath(__file__))

def download_document(url):
    try:
        r = requests.get(url)
    except Exception as e:
        print(e)

    pdf_name = url.split('/')[-1]
    path = dir_path + pdf_name
    with open(path, 'wb') as file:
        file.write(r.content)
    return pdf_name


def process_document():

    # Convert to in-memory binary streams and read pdf
    with open(dir_path +  '\\tmp\\20_9420131_1542190716242.pdf', "rb") as file:
        pdfReader = PyPDF2.PdfFileReader(file,strict = True)

        pdf_pages = map(lambda x: pdfReader.getPage(x).extractText(),range(pdfReader.numPages))
        pdf_text = reduce(lambda a,b: a + b, pdf_pages,'')
        print (read_text)
        return pdf_text if pdf_text else ''
       



download_document('https://dokument.plandata.dk/20_9420131_1542190716242.pdf')