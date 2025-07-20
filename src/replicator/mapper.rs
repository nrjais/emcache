use rusqlite::{
    ToSql,
    types::{ToSqlOutput, Value},
};
use serde_json::Value as JsonValue;

use crate::types::{Entity, Oplog};

#[derive(Debug)]
pub enum SqlValue {
    Null,
    Bool(bool),
    Number(f64),
    Integer(i64),
    String(String),
    Bytes(Vec<u8>),
}

impl ToSql for SqlValue {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self {
            SqlValue::Null => Ok(ToSqlOutput::Owned(Value::Null)),
            SqlValue::Bool(b) => Ok(ToSqlOutput::Owned(Value::Integer(if *b { 1 } else { 0 }))),
            SqlValue::Number(n) => Ok(ToSqlOutput::Owned(Value::Real(*n))),
            SqlValue::Integer(n) => Ok(ToSqlOutput::Owned(Value::Integer(*n))),
            SqlValue::String(s) => Ok(ToSqlOutput::Owned(Value::Text(s.clone()))),
            SqlValue::Bytes(b) => Ok(ToSqlOutput::Owned(Value::Blob(b.clone()))),
        }
    }
}

impl TryFrom<&JsonValue> for SqlValue {
    type Error = anyhow::Error;

    fn try_from(value: &JsonValue) -> Result<Self, Self::Error> {
        match value {
            JsonValue::Null => Ok(SqlValue::Null),
            JsonValue::Bool(b) => Ok(SqlValue::Bool(*b)),
            JsonValue::Number(number) if number.is_f64() => Ok(SqlValue::Number(number.as_f64().unwrap())),
            JsonValue::Number(number) if number.is_i64() => Ok(SqlValue::Integer(number.as_i64().unwrap())),
            JsonValue::Number(number) if number.is_u64() => Ok(SqlValue::Integer(number.as_u64().unwrap() as i64)),
            JsonValue::String(s) => Ok(SqlValue::String(s.clone())),
            _ => {
                let json_str = serde_json::to_vec(value)?;
                Ok(SqlValue::Bytes(json_str))
            }
        }
    }
}

pub fn generate_insert_query(
    entity: &Entity,
    oplog: &Oplog,
    table_name: &str,
) -> anyhow::Result<(String, Vec<SqlValue>)> {
    let mut values = Vec::new();
    let mut placeholders = String::new();
    let mut columns = String::new();

    let id_value = SqlValue::String(oplog.doc_id.clone());

    values.push(id_value);
    columns.push_str("id");
    placeholders.push('?');

    let data = oplog.data.as_array().unwrap();

    for (i, column) in entity.shape.columns.iter().enumerate() {
        let value = data.get(i).unwrap().try_into()?;

        values.push(value);

        placeholders.push_str(", ?");

        columns.push_str(", ");
        columns.push_str(&column.name);
    }

    let query = format!("INSERT OR REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})");

    Ok((query, values))
}
