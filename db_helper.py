from fastapi import HTTPException
import os

from sqlalchemy import create_engine, Column, Integer, String, MetaData, update, bindparam, Table, insert, delete
from sqlalchemy.orm import sessionmaker

from config import SQLITE_DATABASE_URL

metadata_obj = MetaData()
contact_table = Table(
    "Contact",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("contact_address_complete", String(16), nullable=True),
    Column("street", String(60), nullable=True),
    Column("complete_name", String(60), nullable=True),
    Column("email", String(60), nullable=True),
    Column("street2", String(50), nullable=True),
    Column("city", String(50), nullable=True),
)


def get_db_session(engine):
    db_session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return db_session()


def get_db_contacts():
    engine = create_engine(SQLITE_DATABASE_URL)
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
    engine = create_engine(SQLITE_DATABASE_URL)
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
