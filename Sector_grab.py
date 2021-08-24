import xlrd
import requests
from bs4 import BeautifulSoup
import csv
loc = (r"C:\Users\micah\Documents/sustainalytics_data2.xls")
wb = xlrd.open_workbook(loc)
sheet = wb.sheet_by_index(0)

listIndustry = []
i = 1
while i < 4409wddedecdcedeeedce:
    var = str(sheet.cell_value(i, 4))
    url = 'https://www.sustainalytics.com{}'.format(var)
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    #print(url)

    resultsIndustry =(soup.find("strong", {"class": "industry-group"}))

    #for resultsIndustry in soup.find_all('strong', {"class" : "industry-group"}):
    listIndustry.append(resultsIndustry.text)

    i += 1
#print(listIndustry)
data = (list(zip(listIndustry)))
#print(data)
with open(r'C:\Users\micah\documents\sustainalytics_data3.csv','w',newline='') as out:
    csv_out=csv.writer(out)
    csv_out.writerow(['Industry'])
    for row in data:
        csv_out.writerow(row)
