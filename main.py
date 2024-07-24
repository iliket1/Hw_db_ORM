import json

import sqlalchemy
from sqlalchemy.orm import sessionmaker

from models import create_tables, Publisher, Shop, Book, Stock, Sale

DSN = 'postgresql://postgres:postgres@localhost:5432/books_db'
engine = sqlalchemy.create_engine(DSN)
create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

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

pub_name = input()

result = session.query(Book)\
   .with_entities(Book.title, Shop.name, Sale.date_sale, Sale.price, Sale.count)\
   .join(Publisher, Publisher.id == Book.id_publisher)\
   .join(Stock, Stock.id_book == Book.id)\
   .join(Shop, Shop.id == Stock.id_shop)\
   .join(Sale, Sale.id_stock == Stock.id)\
   .filter(Publisher.name == pub_name).all()

for title, name, date_sale, price, count in result:
   print(f'Title: {title}, Name: {name}, Date: {date_sale}, Amount: {price * count}')
