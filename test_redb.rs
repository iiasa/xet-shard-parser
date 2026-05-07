use redb::{Database, TableDefinition, TableError};
fn test_error() {
    let err: TableError = TableError::TableDoesNotExist(TableDefinition::<u32, u32>::new("x").into());
}
