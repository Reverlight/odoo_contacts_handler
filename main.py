import uvicorn
from fastapi import FastAPI


from fastapi_utilities import repeat_every

from cron_job import sync_database_with_odoo_api
from db_helper import get_db_contacts, get_db_contact

app = FastAPI()


@app.on_event('startup')
@repeat_every(seconds=60)
def run_cron_job():
    sync_database_with_odoo_api()


@app.get('/contracts/{contract_id}')
def get_contact(contract_id):
    return {'contract': get_db_contact(contract_id)}


@app.get('/')
def get_contacts():
    return {'contracts': get_db_contacts()}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
