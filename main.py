from typing import Dict, Any

import uvicorn
from fastapi import FastAPI


from fastapi_utilities import repeat_every
from sqlalchemy import create_engine

from config import SQLITE_DATABASE_URL
from cron_job import sync_database_with_odoo_api
from db_helper import get_db_contacts, get_db_contact, get_db_session
from helper import format_dict_to_list

app = FastAPI()


@app.on_event('startup')
@repeat_every(seconds=60)
async def run_cron_job():
    # Note: fastapi_utilities does not support
    # lifespan events, that's why fastAPI old events is used
    sync_database_with_odoo_api()


@app.get('/contacts/{contact_id}')
async def get_contact(contact_id: int) -> Dict[str, Any]:
    engine = create_engine(SQLITE_DATABASE_URL)
    db_session = get_db_session(engine)
    return {'contact': format_dict_to_list(get_db_contact(db_session, contact_id))[0]}


@app.get('/')
async def get_contacts() -> Dict[str, Any]:
    engine = create_engine(SQLITE_DATABASE_URL)
    db_session = get_db_session(engine)
    return {'contacts': format_dict_to_list(get_db_contacts(db_session))}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
