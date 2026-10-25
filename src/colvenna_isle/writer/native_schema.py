"""Native catalog identity shared by mutation, publication and controlled reads."""


def physical(connection, dataset):
    schema, table = dataset.split(".")
    condition = "database_name='warehouse' AND schema_name=? AND table_name=?"
    parameters = [schema, table]
    return {
        "columns": connection.execute(
            "SELECT column_name,data_type,is_nullable,column_default FROM duckdb_columns() WHERE "
            + condition + " ORDER BY column_index", parameters).fetchall(),
        "constraints": connection.execute(
            "SELECT constraint_type,constraint_text,constraint_column_names,referenced_table,"
            "referenced_column_names FROM duckdb_constraints() WHERE " + condition
            # Every NOT NULL has the same text: the column names make the order total.
            + " ORDER BY constraint_type,constraint_text,constraint_column_names", parameters).fetchall(),
        "indexes": connection.execute(
            "SELECT index_name,is_unique,is_primary,expressions,sql FROM duckdb_indexes() WHERE "
            + condition + " ORDER BY index_name", parameters).fetchall(),
    }
