from fastapi import HTTPException

from sqlalchemy import Column, Integer, String, MetaData, Table
from sqlalchemy.orm import sessionmaker


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


def get_db_contacts(db_session):
    result = db_session.query(contact_table).all()
    db_contacts_dict = {item.id: {
        **{key: getattr(item, key) for key in contact_table.columns.keys()},
        '_id': item.id,
    } for item in result}
    return db_contacts_dict


def get_db_contact(db_session, contract_id):
    result = db_session.query(contact_table).filter(contact_table.c.id==contract_id).all()
    if not result:
        raise HTTPException(status_code=404, detail="Item not found")
    contract = result[0]
    db_contact_dict = {contract.id: {
        **{key: getattr(contract, key) for key in contact_table.columns.keys()},
        '_id': contract.id,
    }}
    return db_contact_dict
