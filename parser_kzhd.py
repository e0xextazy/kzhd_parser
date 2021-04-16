from functools import partial
import requests
from bs4 import BeautifulSoup
import re
from requests.utils import requote_uri
import pandas as pd
from multiprocessing import Pool
from datetime import date, timedelta, datetime

ways = [[2708001, 2700000], [2708001, 2708952], [2708001, 2708930], [2708001, 2704600], [2700000, 2708001], [2700000, 2708952], [2700000, 2708930], [2700000, 2704600], [2708952, 2708001], [2708952, 2700000], [2708952, 2708930], [2708952, 2704600], [2708930, 2708001], [2708930, 2700000], [2708930, 2708952], [2708930, 2704600], [2704600, 2708001], [2704600, 2700000], [2704600, 2708952], [2704600, 2708930]]

columns = ['way', 'date', 'train_number', 'car_number', 'car_class', 'car_category', 'carrier', 'seats_count']

url1 = ('https://bilet.railways.kz/sale/default/route/search?'
        'route_search_form%5BdepartureStation%5D={}' # from
        '&route_search_form%5BarrivalStation%5D={}' # to
        '&route_search_form%5BforwardDepartureDate%5D={}%2C+%D1%81%D0%B1%D1%82'
        '&route_search_form%5BbackwardDepartureDate%5D='
        )

url2 = ('https://bilet.railways.kz/sale/default/car/search?'
        'car_search_form%5BdepartureStation%5D={}' # from
        '&car_search_form%5BarrivalStation%5D={}' # to
        '&car_search_form%5BforwardDirection%5D%5BdepartureTime%5D={}T{}' # date and time
        '&car_search_form%5BforwardDirection%5D%5BfluentDeparture%5D='
        '&car_search_form%5BforwardDirection%5D%5Btrain%5D={}' # train number
        '&car_search_form%5BforwardDirection%5D%5BisObligativeElReg%5D=0'
        '&car_search_form%5BbackwardDirection%5D%5BdepartureTime%5D='
        '&car_search_form%5BbackwardDirection%5D%5BfluentDeparture%5D='
        '&car_search_form%5BbackwardDirection%5D%5Btrain%5D='
        '&car_search_form%5BbackwardDirection%5D%5BisObligativeElReg%5D='
        )

def inverse_date(d):
    return datetime.strptime(str(d), '%Y-%m-%d').strftime('%d-%m-%Y')

def parse_way(date, way):
    frm, to = way
    data = []
    response1 = requests.get(url1.format(frm, to, inverse_date(date)))
    soup1 = BeautifulSoup(response1.text, features="html.parser")
    train_numbers = []
    for x in [re.findall(r'[0-9]{3}.', number.text.strip())[0] for number in soup1.find_all('h3', title=re.compile('№'))[::2]]:
        if x == '7556':
            train_numbers.append('856Й')
        elif x == '044*':
            train_numbers.append('043Ц')
        elif x == '6858':
            train_numbers.append('858А')
        elif x == '075*':
            train_numbers.append('076Х')
        elif x == '053*':
            train_numbers.append('054Т')
        elif x == '711*':
            train_numbers.append('712Х')
        elif x == '055*':
            train_numbers.append('056Т')
        elif x == '7553':
            train_numbers.append('853Й')
        elif x == '6857':
            train_numbers.append('857А')
        elif x == '118*':
            train_numbers.append('117Ц')
        else:
            train_numbers.append(x)
    if frm == 2704600:
        times = [re.findall(r'\d{2}:\d{2}', tm.text)[3] for tm in soup1.find_all('div', class_='ui mobile only three equal width grid')]
    else:
        times = [re.findall(r'\d{2}:\d{2}', tm.text)[0] for tm in soup1.find_all('div', class_='ui mobile only three equal width grid')]
    for i in range(0, len(train_numbers)):
        response2 = requests.get(url2.format(frm, to, str(date), requote_uri(times[i]), requote_uri(train_numbers[i])))
        soup2 = BeautifulSoup(response2.text, features="html.parser")
        way = soup2.find('span', class_='train-header train-route ktj link info').text.strip()
        cells = soup2.find_all('tr', class_='title')
        for cell in cells:
            cell_info = [tmp.text.strip() for tmp in cell.find_all('td')]
            car_category = re.sub(r' +', ' ', cell_info[2].replace('\n', ''))
            data.append([way, str(date), train_numbers[i], cell_info[0], cell_info[1], car_category, cell_info[3], cell_info[-1]])
    return data

if __name__ == '__main__':
    df = pd.DataFrame(columns=columns)
    for i in range(6, 7):
        date = date.today() + timedelta(days=i)
        with Pool(1) as p:
            func = partial(parse_way, date)
            all_data = p.map(func, ways)
        for data in all_data:
            df = pd.concat([df, pd.DataFrame(data, columns=columns)])
    df.to_csv('/data.csv')
   