from random import randrange, choice, randint
from pandas import DataFrame
from pyarrow import Table
import pyarrow.parquet as pq
import os
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# --- Assets & Constants moved from assets.py ---

CHANNELS = [{'channel_name':  'in-store', 'channel_id': 1},
          {'channel_name':  'web', 'channel_id': 2},
          {'channel_name':  'mobile app', 'channel_id': 3}]

CITIES = ['Moskva (Moscow)', 'London', 'St Petersburg', 'Berlin', 'Madrid', 'Roma', 'Kiev', 'Paris', 'Bucuresti (Bucharest)', 'Budapest', 'Hamburg', 'Minsk', 'Warszawa (Warsaw)', 'Beograd (Belgrade)', 'Wien (Vienna)', 'Kharkov', 'Barcelona', 'Novosibirsk', 'Nizhny Novgorod', 'Milano (Milan)', 'Ekaterinoburg', 'München (Munich)', 'Praha (Prague)', 'Samara', 'Omsk', 'Sofia', 'Dnepropetrovsk', 'Kazan', 'Ufa', 'Chelyabinsk', 'Donetsk ', 'Napoli (Naples)', 'Birmingham', 'Perm', 'Rostov-Na-Donu', 'Odessa', 'Volgograd', 'Köln (Cologne)', 'Torino (Turin)', 'Voronezh', 'Krasnoyarsk', 'Saratov', 'Zagreb', 'Zaporozhye', 'Lódz', 'Marseille', 'Riga', 'Lvov', 'Athinai (Athens)', 'Salonika', 'Stockholm', 'Kraków', 'Valencia', 'Amsterdam', 'Leeds', 'Tolyatti', 'Kryvy Rig', 'Sevilla', 'Palermo', 'Ulyanovsk', 'Kishinev', 'Genova', 'Izhevsk', 'Frankfurt Am Main', 'Krasnodar', 'Wroclaw (Breslau)', 'Glasgow', 'Yaroslave', 'Khabarovsk', 'Vladivostok', 'Zaragoza', 'Essen', 'Rotterdam', 'Irkutsk', 'Dortmund', 'Stuttgart', 'Barnaul', 'Vilnius', 'Poznan', 'Düsseldorf', 'Novokuznetsk', 'Lisboa (Lisbon)', 'Helsinki', 'Málaga', 'Bremen', 'Sheffield', 'Sarajevo', 'Penza', 'Ryazan', 'Orenburg', 'Naberezhnye Tchelny', 'Duisburg', 'Lipetsk', 'Hannover', 'Mykolaiv ', 'Tula', 'Oslo', 'Tyumen', 'Kobenhavn (Copenhagen)', 'Kemerovo']

RESELLERS_TRANSACTIONS = [
{'reseller_id' : 1001, 'reseller_name': 'Imaginary Street Press Company','commission_pct': 0.15},
{'reseller_id' : 1002, 'reseller_name': 'European Example Press Corporation','commission_pct': 0.17},
{'reseller_id' : 1003, 'reseller_name': 'Scandinavian Legendary Printing Company','commission_pct': 0.14},
{'reseller_id' : 1004, 'reseller_name': 'Mediterranean Postcard Press Association','commission_pct': 0.16}
]

def get_channel_distribution(channel):
    if channel == 'direct':
        return [*2*('in-store',), *2*('web',), *3*('mobile app',) ]
    elif channel == 'reseller':
        return [*1*('in-store',), *3*('web',), *3*('mobile app',) ]

def random_date(start=datetime(2019,1,1), end=datetime(2021,1,31)):
    """Generate a random datetime between `start` and `end`"""
    result =  start + timedelta(
        # Get a random amount of seconds between `start` and `end`
        seconds=randint(0, int((end - start).total_seconds())),)
    return result.date()

# --- Product Generation ---

PRODUCTS = []
PRODUCT_FORMATS = ['4x6', '4.25x6', '5x7', '5.5x8.5', '6x9', '6x11']

print('Generating product definition...')
for i in range(500):
    city = choice(CITIES)
    format = choice(PRODUCT_FORMATS)
    catchphrase = fake.catch_phrase()
    # Ensure name isn't too long or crazy, but catch_phrase is good.
    # Pattern: Format + City + Catchphrase snippet or similar
    name = f"{format} - {city} - {catchphrase}"
    
    # Simple consistent ID generation attempt, but unique enough
    product_id = f"PROD-{i:04d}-{city[:3].upper()}"
    
    PRODUCTS.append({
        'name': name,
        'city': city,
        'price': randrange(15,45)/10.0,
        'product_id': product_id
    })


# --- Generators ---

def generate_main(n=1000000):
    print('Generating transactions')

    trans = []

    for i in range(n):

        product = choice(PRODUCTS)
        
        bought = random_date()
        boughtdate = str(bought)

        qty = randrange(1,6)

        transaction = {
        	       'transaction_id': i,
        	       'customer_id': randrange(1,100000),
                       'product_id': product['product_id'],
                       'amount': product['price'] * qty,
                       'qty': qty,
                       'channel_id': choice([i['channel_id'] for i in CHANNELS]),
                       'bought_date': boughtdate }

        trans.append(transaction)

    generate_parquet_file('main', trans)

def generate_resellers():
    print('Generating resellers')

    generate_parquet_file('resellers', RESELLERS_TRANSACTIONS)

def generate_channels():
    print('Generating channels')

    generate_parquet_file('channels', CHANNELS)


def generate_customers():
    print('Generating customers')

    trans = []
    for i in range(100000):
        first_name = fake.first_name()
        last_name = fake.last_name()
        trans.append({
            'customer_id': i, 
            'first_name': first_name , 
            'last_name': last_name, 
            'email': f'{first_name}.{last_name}@{fake.free_email_domain()}',
            'phone_number': fake.phone_number(),
            'address': fake.street_address(),
            'city': fake.city(),
            'country': fake.country(),
            'postal_code': fake.postcode()
        })

    generate_parquet_file('customers', trans)


def generate_products():
    print('Generating products')
    generate_parquet_file('products', PRODUCTS)


def generate_type1_reseller_data(n=50000):
    print('Generating Type 1 Reseller data')

    export = []

    for resellerid in [1001, 1002]:

        for i in range(n): 

            product = choice(PRODUCTS)
            qty = randrange(1,7)
            boughtdate = str(random_date())
            first_name = fake.first_name()
            last_name = fake.last_name()

            transaction = {
                        'Product name': product['name'],
                        'Quantity':  qty,
                        'Total amount': qty * product['price'],
                        'Sales Channel': choice(get_channel_distribution('reseller')),
                        'Customer First Name': first_name,
                        'Customer Last Name': last_name,
                        'Customer Email': f'{first_name}.{last_name}@{fake.free_email_domain()}',
                        'Customer Phone': fake.phone_number(),
                        'Customer Address': fake.street_address(),
                        'Customer City': fake.city(),
                        'Customer Country': fake.country(),
                        'Customer Postal Code': fake.postcode(),
                        'Series City': product['city'],
                        'Created Date': boughtdate,
                        'Reseller ID' : resellerid,
                        'Transaction ID': i
                        }

            export.append(transaction)

    generate_parquet_file('resellers_type1', export)


def generate_type2_reseller_data(n = 50000 ):
    print('Generating Type 2 reseller data')
    export = []

    for resellerid in [1003, 1004]:
        for i in range(n):

            product = choice(PRODUCTS)
            qty = randrange(1,7)
            bought = random_date()
            boughtdate = str(bought).replace('-','')
            first_name = fake.first_name()
            last_name = fake.last_name()

            transaction = {
                        'date': boughtdate,
                        'reseller-id':resellerid,
                        'productName': product['name'],
                        'qty' : qty,
                        'totalAmount': qty * product['price'] * 1.0,
                        'salesChannel': choice(get_channel_distribution('reseller')),
                        'customer': {
                            'firstname': first_name, 
                            'lastname': last_name, 
                            'email': f'{first_name}.{last_name}@{fake.free_email_domain()}',
                            'phone': fake.phone_number(),
                            'address': fake.street_address(),
                            'city': fake.city(),
                            'country': fake.country(),
                            'postal': fake.postcode()
                        },
                        'dateCreated': boughtdate,
                        'seriesCity': product['city'],
                        'Created Date': str(bought),
                        'transactionID': i
                        }
            export.append(transaction)

    generate_parquet_file('resellers_type2', export)

def cleanup(directory, ext):
    import os
    filelist = [ f for f in os.listdir(directory) if f.endswith(ext) ]
    for f in filelist:
        os.remove(os.path.join(directory, f))


def generate_parquet_file(name, records):
    df = DataFrame(records)
    table = Table.from_pandas(df)
    pq.write_table(table, f"{os.environ['INPUT_FILES_PATH']}/{name}.parquet")

n_transactions = int(os.environ.get('N_TRANSACTIONS', 1000000))

generate_main(n=n_transactions)
generate_channels()
generate_customers()
generate_products()
generate_resellers()
generate_type1_reseller_data()
generate_type2_reseller_data()
