from pdfminer.pdfinterp import PDFResourceManager, process_pdf
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from io import StringIO
from PyPDF2 import PdfFileReader
import os
import requests


def pdfminer_extract_text_from_url(url):
    # Download pdf
    r = requests.get(url)
    with open('file.pdf', 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)

    # Extract text
    pdfFile = open('file.pdf', 'rb')

    rsrcmgr = PDFResourceManager()
    retstr = StringIO()
    laparams = LAParams()
    device = TextConverter(rsrcmgr, retstr, laparams=laparams)
    process_pdf(rsrcmgr, device, pdfFile)
    device.close()
    content = retstr.getvalue()
    retstr.close()

    os.remove('file.pdf')

    return content

def pypdf_extract_text_from_url(url):
    # Download pdf
    r = requests.get(url)

    with open('file.pdf', 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)

    # Extract text
    f = open('file.pdf', 'rb')

    reader = PdfFileReader(f)
    content = ''
    for i in reader.pages:
        content += i.extractText()
    f.close()

    os.remove('file.pdf')

    return content
