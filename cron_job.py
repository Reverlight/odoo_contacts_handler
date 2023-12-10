import xmlrpc.client
from typing import Dict, Any, Optional, List
import logging

from sqlalchemy import create_engine, update, bindparam, insert, delete
from sqlalchemy.orm import Session


from config import ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD, ODOO_URL, SQLITE_DATABASE_URL
from db_helper import contact_table, get_db_session, get_db_contacts
from helper import format_dict_to_list

logger = logging.getLogger(__name__)


def _db_sync_contacts(
    db_session: Session,
    odoo_contacts: Dict[str, Dict[str, Any]],
    added=Optional[List[int]],
    deleted=Optional[List[int]],
    modified=Optional[List[int]],
):
    # Synchronizes contracts in database by executing SQL for adding, deleting and modifying rows
    if added:
        stmt = (
            insert(contact_table).
            values([
                get_contacts_with_correct_id_key(odoo_contacts[item_id])
                for item_id in added
            ])
        )
        db_session.execute(stmt)

    if modified:
        params = {key: bindparam(key) for key in contact_table.columns.keys()}
        stmt = update(contact_table).where(contact_table.c.id == bindparam('_id')).values(params)
        modified_values = format_dict_to_list(modified)

        db_session.execute(
            stmt,
            modified_values
        )

    if deleted:
        stmt = (
            delete(contact_table).
            filter(contact_table.c.id.in_(deleted))
        )
        db_session.execute(stmt)

    db_session.commit()


def get_odoo_contacts() -> Dict[str, Dict[str, Any]]:
    try:
        common = xmlrpc.client.ServerProxy('{}/common'.format(ODOO_URL))
        uid = common.authenticate(ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD, {})

        models = xmlrpc.client.ServerProxy('{}/object'.format(ODOO_URL))
        ids = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD, 'res.partner', 'search', [[]])
    except Exception as ex:
        message = f'Request was failed {repr(ex)}'
        logger.error(message)
        raise ex(message)

    odoo_contacts = models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD, 'res.partner', 'read', [ids])
    odoo_contracts = {contact['id']: {
        **{key: contact[key] for key in contact_table.columns.keys()},
        '_id': contact['id'],
    } for contact in odoo_contacts}

    return odoo_contracts


def compare_contacts(odoo_dict: Dict[str, Dict[str, Any]], db_dict: Dict[str, Dict[str, Any]]):
    ids_to_insert = set(odoo_dict.keys()) - set(db_dict.keys())
    ids_to_delete = set(db_dict.keys()) - set(odoo_dict.keys())
    contacts_to_modify = {
        key: odoo_dict[key]
        for key in set(db_dict.keys()) & set(odoo_dict.keys())
        if db_dict[key] != odoo_dict[key]
    }
    return ids_to_insert, ids_to_delete, contacts_to_modify


def get_contacts_with_correct_id_key(contracts: Dict[str, Dict[str, Any]]):
    # _id is used inertly in sqlalchemy
    # but in some cases we would need exactly id
    # For example in case of insert into database using id from Odoo
    new_dict = contracts.copy()
    new_dict['id'] = new_dict.pop('_id')
    return new_dict


def sync_database_with_odoo_api():
    engine = create_engine(SQLITE_DATABASE_URL)
    # Create the table if it doesn't exist
    contact_table.metadata.create_all(bind=engine)
    odoo_contacts = get_odoo_contacts()
    db_session = get_db_session(engine)
    db_contacts = get_db_contacts(db_session)
    ids_to_insert, ids_to_delete, contacts_to_modify = compare_contacts(odoo_contacts, db_contacts)
    _db_sync_contacts(
        db_session,
        odoo_contacts,
        ids_to_insert,
        ids_to_delete,
        contacts_to_modify,
    )
