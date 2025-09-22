# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pyodbc


class BookscraperPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        ## Strip all whitespaces from strings
        field_names = adapter.field_names()
        for field_name in field_names:
            if field_name != 'description':
                value = adapter.get(field_name)
                adapter[field_name] = value.strip()
        
        ## Category & Product Type --> switch to lowercase
        lowercase_keys = ['category', 'product_type']
        for lowercase_key in lowercase_keys:
            value = adapter.get(lowercase_key)
            adapter[lowercase_key] = value.lower()

        ## Price --> convert to float
        price_keys = ['price', 'price_excl_tax', 'price_incl_tax', 'tax']
        for price_key in price_keys:
            value = adapter.get(price_key)
            value = value.replace('£', '')
            adapter[price_key] = float(value)

        ## Availability --> extract number of books in stock
        availability_string = adapter.get('availability')    # e.g. 'In stock (22 available)'
        split_string_array = availability_string.split('(')  # splits the availability string at the '(' character
        if len(split_string_array) < 2:  # if there is no '(' character in the string e.g 'out of stock'
            adapter['availability'] = 0
        else:
            availability_array = split_string_array[1].split(' ') # splits the second part of the string at the ' ' character
             # e.g. ['22', 'available)']
            adapter['availability'] = int(availability_array[0]) # extracts the first part of string i.e '22' and converts it to integer
        
        ## Reviews --> convert string to number
        num_reviews_string = adapter.get('num_reviews')
        adapter['num_reviews'] = int(num_reviews_string)
        
        ## Stars --> convert text to number
        stars_string = adapter.get('stars')
        split_stars_array = stars_string.split(' ')
        stars_text_value = split_stars_array[1].lower()
        if stars_text_value == "zero":
            adapter['stars'] = 0
        elif stars_text_value == "one":
            adapter['stars'] = 1
        elif stars_text_value == "two":
            adapter['stars'] = 2
        elif stars_text_value == "three":
            adapter['stars'] = 3
        elif stars_text_value == "four":
            adapter['stars'] = 4
        elif stars_text_value == "five":
            adapter['stars'] = 5

        return item

class SaveToMSSQLPipeline:
    def open_spider(self, spider):
        # Connect to SQL Server using Windows Authentication
        self.conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=localhost;"            # Use localhost\\SQLEXPRESS if using SQL Server Express
            "DATABASE=Bookstore;"              #  database name
            "Trusted_Connection=yes;"      # Windows Authentication
        )
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        # Insert scraped data into your table
        self.cursor.execute("""
            INSERT INTO BookItems (url, title, upc, product_type, price_excl_tax, price_incl_tax,
                                   tax, availability, num_reviews, stars, category, description, price)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            item['url'],
            item['title'],
            item['upc'],
            item['product_type'],
            item['price_excl_tax'],
            item['price_incl_tax'],
            item['tax'],
            item['availability'],
            item['num_reviews'],
            item['stars'],
            item['category'],
            item['description'],
            item['price']
        ))
        self.conn.commit()
        return item

    def close_spider(self, spider):
        # Close the connection when spider finishes
        self.conn.close()

import csv

class SplitBookItemPipeline:
    def open_spider(self, spider):
        # Open two CSV files
        self.master_file = open('master.csv', 'w', newline='', encoding='utf-8')
        self.meta_file = open('meta.csv', 'w', newline='', encoding='utf-8')

        # Master and Meta fieldnames
        self.master_fields = ['id', 'url', 'title', 'price', 'availability', 'stars']
        self.meta_fields = ['master_id', 'upc', 'product_type', 'price_excl_tax', 'price_incl_tax', 'tax', 'num_reviews', 'category', 'description']

        # Writers
        self.master_writer = csv.DictWriter(self.master_file, fieldnames=self.master_fields)
        self.meta_writer = csv.DictWriter(self.meta_file, fieldnames=self.meta_fields)

        self.master_writer.writeheader()
        self.meta_writer.writeheader()

        self.counter = 1  # unique id for each book

    def process_item(self, item, spider):
        # Split data into two tables
        master_row = {
            'id': self.counter,
            'url': item['url'],
            'title': item['title'],
            'price': item['price'],
            'availability': item['availability'],
            'stars': item['stars'],
        }

        meta_row = {
            'master_id': self.counter,
            'upc': item['upc'],
            'product_type': item['product_type'],
            'price_excl_tax': item['price_excl_tax'],
            'price_incl_tax': item['price_incl_tax'],
            'tax': item['tax'],
            'num_reviews': item['num_reviews'],
            'category': item['category'],
            'description': item['description'],
        }

        # Write to CSV
        self.master_writer.writerow(master_row)
        self.meta_writer.writerow(meta_row)

        self.counter += 1
        return item

    def close_spider(self, spider):
        self.master_file.close()
        self.meta_file.close()
