import os

from dotenv import load_dotenv

load_dotenv()

ODOO_URL = os.getenv('ODOO_URL')
ODOO_DB = os.getenv('ODOO_DB')
ODOO_USERNAME = os.getenv('ODOO_USERNAME')
ODOO_PASSWORD = os.getenv('ODOO_PASSWORD')
SQLITE_DATABASE_URL = os.getenv('SQLITE_DATABASE_URL')
