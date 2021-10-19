import PyPDF2
import io

# pdfFileObj = open(r"C:\Users\Alen\Downloads\Interview_sample_data.pdf", "rb")
pdfFileObj = open(r"C:/Users/Alen/Downloads/my_sample_file.pdf", "rb")
pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
print(pdfReader.numPages)
pageObj = pdfReader.getPage(0)

pages_text= pageObj.extractText()
print(pages_text)
pdfFileObj.close()
