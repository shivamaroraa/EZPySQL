from connection import Database


def query_with_params(db, query, params=None):
    try:
        if params:
            results = db.query(query, params)
        else:
            results = db.query(query)
        return results
    except Exception as e:
        db.rollback()
        raise e


def get_valid_columns(table_name, db_type='sqlite'):
    query = f"PRAGMA table_info({table_name})" if db_type == 'sqlite' else f"SHOW COLUMNS FROM {table_name}"

    with Database(db_type) as db:
        columns = db.query(query)

    if db_type == 'sqlite':
        return [column['name'] for column in columns if
                not (column['pk'] == 1 and column['type'].upper() == 'INTEGER')]
    else:
        return [column['Field'] for column in columns if 'auto_increment' not in column['Extra']]


def get_table_data(table_name, columns=None, order_by=None, desc=False, limit=None, db_type='sqlite', **kwargs):
    result, error = None, None

    if columns:
        columns_str = ", ".join(columns)
    else:
        columns_str = "*"

    query = f"SELECT {columns_str} FROM {table_name}"

    if kwargs:
        conditions = [f"{key} = ?" for key in kwargs.keys()] if db_type == 'sqlite' else [f"{key} = %s" for key in
                                                                                          kwargs.keys()]
        where_clause = " WHERE " + " AND ".join(conditions)
        query += where_clause

    if order_by:
        query += f" ORDER BY {order_by}"
        if desc:
            query += " DESC"

    params = tuple(kwargs.values())

    with Database(db_type) as db:
        try:
            result = query_with_params(db, query, params)
            if limit:
                result = result[0:limit]
                if limit == 1:
                    result = result[0]


        except Exception as e:
            error = str(e)
            print(error)
            db.rollback()

    return result, error


def insert_into_db(table_name, db_type='sqlite', records=None, **kwargs) -> (str, str):
    result, error = None, None

    valid_columns = get_valid_columns(table_name, db_type)

    if records and isinstance(records, list):
        existing_records, _ = get_table_data(table_name, db_type=db_type)

        existing_records_list = existing_records
        filtered_records = [{k: v for k, v in record.items() if k in valid_columns} for record in records]

        filtered_records = [record for record in filtered_records if not any(
            all(record[k] == existing_record[k] for k in record.keys()) for existing_record in existing_records_list)]

        columns = ', '.join(valid_columns)
        placeholders = ', '.join('?' if db_type == 'sqlite' else '%s' for _ in valid_columns)

        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        values = [tuple(record[col] for col in valid_columns) for record in filtered_records]

    elif kwargs:
        filtered_kwargs = {k: v for k, v in kwargs.items() if k in valid_columns}

        columns = ', '.join(filtered_kwargs.keys())
        placeholders = ', '.join('?' if db_type == 'sqlite' else '%s' for _ in filtered_kwargs.keys())

        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        values = [tuple(filtered_kwargs.values())]

    else:
        return "No records or keyword arguments provided for insertion", None

    try:
        with Database(db_type) as db:

            db.executemany(query, values)
            db.commit()
            result = f"{len(values)} row(s) added successfully"
    except Exception as e:
        error = str(e)
        print(error)

    return result, error


def update_in_db(table_name, db_type='sqlite', updates=None, records=None, **kwargs):
    result, error = None, None

    if not updates and not records:
        return "No updates provided for update", None

    if not records and not kwargs:
        return "No conditions provided for update", None

    valid_columns = get_valid_columns(table_name, db_type)

    existing_records = get_table_data(table_name, db_type=db_type)

    existing_records_list = existing_records[0]

    if updates:
        conditions = kwargs

    elif records:
        updates = None
        conditions = None
    else:
        return "Invalid arguments provided for update", None

    if updates and conditions:
        existing_records_list = [record for record in existing_records_list if all(
            record[k] == conditions[k] for k in conditions.keys())]

        if not existing_records_list:
            return "No matching record(s) found for update", None

        if any(all(record[k] == updates[k] for k in updates.keys()) for record in existing_records_list):
            return "The updates are the same as the existing record(s), no update required", None

        filtered_updates = {k: v for k, v in updates.items() if k in valid_columns}
        filtered_conditions = {k: v for k, v in conditions.items() if k in valid_columns}

        set_clause = ', '.join(f"{k} = {'?' if db_type == 'sqlite' else '%s'}" for k in filtered_updates.keys())
        where_clause = ' AND '.join(f"{k} = {'?' if db_type == 'sqlite' else '%s'}" for k in filtered_conditions.keys())

        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
        values = [tuple(filtered_updates.values()) + tuple(filtered_conditions.values())]

    elif records:
        queries = []
        values = []

        for record in records:
            filtered_record = {
                'updates': {k: v for k, v in record['updates'].items() if k in valid_columns},
                'conditions': {k: v for k, v in record['conditions'].items() if k in valid_columns}
            }

            set_clause = ', '.join(
                f"{k} = {'?' if db_type == 'sqlite' else '%s'}" for k in filtered_record['updates'].keys())
            where_clause = ' AND '.join(
                f"{k} = {'?' if db_type == 'sqlite' else '%s'}" for k in filtered_record['conditions'].keys())

            query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
            queries.append(query)

            values.append(tuple(filtered_record['updates'].values()) + tuple(filtered_record['conditions'].values()))

    else:
        return "Invalid arguments provided for update", None

    try:
        with Database(db_type) as db:
            for query, value in zip(queries, values):
                db.execute(query, value)
            db.commit()

    except Exception as e:
        error = str(e)
        print(error)

    return result, error


def delete_from_db(table_name, db_type='sqlite', **kwargs):
    result, error = None, None

    if not kwargs:
        return "No conditions provided for deletion", None

    conditions = [f"{key} = ?" for key in kwargs.keys()] if db_type == 'sqlite' else [f"{key} = %s" for key in
                                                                                      kwargs.keys()]
    where_clause = " WHERE " + " AND ".join(conditions)

    query = f"DELETE FROM {table_name}{where_clause}"
    values = [tuple(kwargs.values())]

    existing_row, _ = get_table_data(table_name, db_type=db_type, **kwargs)
    if len(existing_row):
        try:
            with Database(db_type) as db:
                db.executemany(query, values)
                db.commit()
                result = f"{len(values)} row(s) deleted successfully"
        except Exception as e:
            error = str(e)
            print(error)
    else:
        result, error = "No matching rows for deletion", None

    return result, error
