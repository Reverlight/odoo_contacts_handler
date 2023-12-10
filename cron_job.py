import os
import xmlrpc.client

from sqlalchemy import create_engine, Column, Integer, String, MetaData, update, bindparam, Table, insert, delete
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

# TODO. Add logs. Add typings. Rename functions
# TODO. Refactor to reuse fields and not repeat them (maybe through __table__.columns)

# Odoo
ODOO_URL = os.getenv('ODOO_URL')
ODOO_DB = os.getenv('ODOO_DB')
ODOO_USERNAME = os.getenv('ODOO_USERNAME')
ODOO_PASSWORD = os.getenv('ODOO_PASSWORD')
# SQLITE
SQLITE_DATABASE_URL = os.getenv('SQLITE_DATABASE_URL')

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


def db_process_add_modify_delete(
    db_session,
    odoo_contacts,
    added=None,
    deleted=None,
    modified=None,
):
    if added:
        stmt = (
            insert(contact_table).
            values([change_dict(odoo_contacts[item_id]) for item_id in added])
        )
        db_session.execute(stmt)

    if modified:
        params = {key: bindparam(key) for key in contact_table.columns.keys()}

        stmt = update(contact_table).where(contact_table.c.id == bindparam('_id')).values(params).execution_options(
            synchronize_session=None)

        modified_list = []

        for modified_single in modified.values():
            modified_list.append(modified_single)

        db_session.execute(
            stmt,
            modified_list
        )

    if deleted:
        stmt = (
            delete(contact_table).
            filter(contact_table.c.id.in_(deleted))
        )
        db_session.execute(stmt)


def get_db_contacts(db_session):
    result = db_session.query(contact_table).all()
    db_contacts_dict = {item.id: {
        **{key: getattr(item, key) for key in contact_table.columns.keys()},
        '_id': item.id,
    } for item in result}
    return db_contacts_dict


def get_odoo_contacts():
    common = xmlrpc.client.ServerProxy('{}/common'.format(ODOO_URL))
    uid = common.authenticate(ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD, {})

    # Connect to the Odoo object model
    models = xmlrpc.client.ServerProxy('{}/object'.format(ODOO_URL))
    ids = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD, 'res.partner', 'search', [[]])

    odoo_contacts = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD, 'res.partner', 'read', [ids])
    odoo_contracts = {contact['id']: {
        **{key: contact[key] for key in contact_table.columns.keys()},
        '_id': contact['id'],
    } for contact in odoo_contacts}

    return odoo_contracts


def get_db_session(engine):
    db_session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return db_session()


def compare_contacts(odoo_dict, db_dict):
    added = set(odoo_dict.keys()) - set(db_dict.keys())
    deleted = set(db_dict.keys()) - set(odoo_dict.keys())
    modified = {key: odoo_dict[key] for key in set(db_dict.keys()) & set(odoo_dict.keys()) if
                db_dict[key] != odoo_dict[key]}

    return added, deleted, modified


def change_dict(dict_):
    new_dict = dict_.copy()
    new_dict['id'] = new_dict.pop('_id')
    return new_dict


def sync_database_with_odoo_api():
    engine = create_engine(SQLITE_DATABASE_URL)
    # Create the table if it doesn't exist
    contact_table.metadata.create_all(bind=engine)
    odoo_contacts = get_odoo_contacts()
    db_session = get_db_session(engine)
    db_contacts = get_db_contacts(db_session)
    added, deleted, modified = compare_contacts(odoo_contacts, db_contacts)
    db_process_add_modify_delete(
        db_session,
        odoo_contacts,
        added,
        deleted,
        modified,
    )
    db_session.commit()

# TODO. REMOVE TEST
sync_database_with_odoo_api()
