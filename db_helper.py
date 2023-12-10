import os

from fastapi import HTTPException
from sqlalchemy import create_engine
import os
import xmlrpc.client

from sqlalchemy import create_engine, Column, Integer, String, MetaData, update, bindparam, Table, insert, delete
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

from cron_job import get_db_session
from cron_job import contact_table


def get_db_contacts():
    engine = create_engine(os.getenv('SQLITE_DATABASE_URL'))
    db_session = get_db_session(engine)
    result = db_session.query(contact_table).all()
    db_contracts_dict = {item.id: {
        '_id': item.id,
        'date_start': item.date_start,
        'date_end': item.date_end,
        'contract_type': item.contract_type,
        'job_position': item.job_position,
        'employee_name': item.employee_name,  # fix to other
        'working_schedule': item.working_schedule,
    } for item in result}
    return db_contracts_dict


def get_db_contact(contract_id):
    engine = create_engine(os.getenv('SQLITE_DATABASE_URL'))
    db_session = get_db_session(engine)
    result = db_session.query(contact_table).filter(contact_table.c.id == contract_id).all()

    if not result:
        raise HTTPException(status_code=404, detail="Item not found")

    db_contracts_dict = {item.id: {
        '_id': item.id,
        'date_start': item.date_start,
        'date_end': item.date_end,
        'contract_type': item.contract_type,
        'job_position': item.job_position,
        'employee_name': item.employee_name,  # fix to other
        'working_schedule': item.working_schedule,
    } for item in result}
    return db_contracts_dict
