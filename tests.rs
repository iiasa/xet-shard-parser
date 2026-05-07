use redb::TableError;

fn match_error(e: TableError) {
    match e {
        TableError::TableDoesNotExist(_) => {},
        _ => {}
    }
}
