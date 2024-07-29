import json

import sqlalchemy
from sqlalchemy.orm import sessionmaker

from models import create_tables, Publisher, Shop, Book, Stock, Sale

DSN = 'postgresql://postgres:postgres@localhost:5432/books_db'
engine = sqlalchemy.create_engine(DSN)
create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

try:
   with open('tests_data.json', 'r') as file:
      data = json.load(file)

   for record in data:
      model = {
         'publisher': Publisher, 
         'shop': Shop, 
         'book': Book, 
         'stock': Stock, 
         'sale': Sale,
      }[record.get('model')]
      session.add(model(id=record.get('pk'), **record.get('fields')))

   session.commit()

except FileNotFoundError:
   print("Файл 'tests_data.json' не найден.")

except json.JSONDecodeError:
   print("Ошибка декодирования JSON.")

# pub_name = input()

# result = session.query(Book)\
#    .with_entities(Book.title, Shop.name, Sale.date_sale, Sale.price, Sale.count)\
#    .join(Publisher, Publisher.id == Book.id_publisher)\
#    .join(Stock, Stock.id_book == Book.id)\
#    .join(Shop, Shop.id == Stock.id_shop)\
#    .join(Sale, Sale.id_stock == Stock.id)\
#    .filter(Publisher.name == pub_name).all()

# for title, name, date_sale, price, count in result:
#    print(f'Title: {title}, Name: {name}, Date: {date_sale}, Amount: {price * count}')

def get_shops(search):

   result = session.query(Book.title, Shop.name, Sale.date_sale, Sale.price, Sale.count)\
      .select_from(Shop)\
      .join(Stock)\
      .join(Book)\
      .join(Publisher)\
      .join(Sale)
   
   if search.isdigit():
      search_pub = result.filter(Publisher.id == search).all()
   else:
      search_pub = result.filter(Publisher.name.like(f'%{search}%')).all()

   for title, name, date_sale, price, count in search_pub:
      print(f"{title: <40} | {name: <10} | {price * count: <8} | {date_sale.strftime('%d-%m-%Y')}")


if __name__ == '__main__':

   search = input('Введите имя или ID публициста: ')
   get_shops(search)