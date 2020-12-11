from genefab3.common.exceptions import GeneLabDatabaseException


REPLACE_ERROR = "run_mongo_transaction('replace') without a query and/or data"
DELETE_MANY_ERROR = "run_mongo_transaction('delete_many') without a query"
INSERT_MANY_ERROR = "run_mongo_transaction('insert_many') without documents"
ACTION_ERROR = "run_mongo_transaction() with an unsupported action"


def run_mongo_transaction(action, collection, query=None, data=None, documents=None):
    """Shortcut to drop all instances and replace with updated instance"""
    with collection.database.client.start_session() as session:
        with session.start_transaction():
            if action == "replace":
                if (query is not None) and (data is not None):
                    collection.delete_many(query)
                    collection.insert_one({**query, **data})
                else:
                    raise GeneLabDatabaseException(REPLACE_ERROR)
            elif action == "delete_many":
                if query is not None:
                    collection.delete_many(query)
                else:
                    raise GeneLabDatabaseException(DELETE_MANY_ERROR)
            elif action == "insert_many":
                if documents is not None:
                    collection.insert_many(documents)
                else:
                    raise GeneLabDatabaseException(INSERT_MANY_ERROR)
            else:
                raise GeneLabDatabaseException(ACTION_ERROR, action=action)
