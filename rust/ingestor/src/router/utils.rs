use anyhow::Result;
use base64::{Engine, engine::general_purpose};
use bigdecimal::BigDecimal;
use scylla::{client::session::Session, value::CqlValue};
use serde_json::{Map, Value};
use std::hash::Hasher;
use twox_hash::XxHash64;
use uuid::Uuid;

pub(super) fn serialize_row_to_json(
    row: &scylla_cdc::consumer::CDCRow<'_>,
) -> anyhow::Result<String> {
    let mut doc = Map::new();

    doc.insert(
        "_cdc_stream_id".to_string(),
        Value::String(row.stream_id.to_string()),
    );
    doc.insert("_cdc_time".to_string(), Value::String(row.time.to_string()));
    doc.insert(
        "_cdc_batch_seq".to_string(),
        Value::Number(row.batch_seq_no.into()),
    );

    if let Some(ttl) = row.ttl {
        doc.insert("_cdc_ttl".to_string(), Value::Number(ttl.into()));
    }

    for column_name in row.get_non_cdc_column_names() {
        if let Some(cql_value) = row.get_value(column_name) {
            let json_value = cql_to_json(cql_value);
            doc.insert(column_name.to_string(), json_value);
        }
    }

    Ok(serde_json::to_string(&doc)?)
}

pub(super) fn extract_writetime_from_timeuuid(time_uuid: Uuid) -> anyhow::Result<u64> {
    let ts = time_uuid
        .get_timestamp()
        .ok_or_else(|| anyhow::anyhow!("Not a time-based UUID"))?;

    let (secs, nanos) = ts.to_unix();
    let micros = (secs * 1_000_000) + (nanos as u64 / 1_000);

    Ok(micros)
}

fn cql_to_json(val: &CqlValue) -> Value {
    match val {
        CqlValue::Ascii(s) | CqlValue::Text(s) => Value::String(s.clone()),
        CqlValue::Boolean(b) => Value::Bool(*b),
        CqlValue::Int(i) => Value::Number((*i).into()),
        CqlValue::BigInt(i) => Value::Number((*i).into()),
        CqlValue::Decimal(d) => {
            let bd: BigDecimal = d.clone().into();
            Value::String(bd.to_string())
        }
        CqlValue::Double(f) => serde_json::Number::from_f64(*f)
            .map(Value::Number)
            .unwrap_or(Value::Null), // Handle NaN/Infinite
        CqlValue::Float(f) => serde_json::Number::from_f64(*f as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        CqlValue::Uuid(u) => Value::String(u.to_string()),
        CqlValue::Timeuuid(t) => Value::String(t.to_string()),
        CqlValue::Timestamp(ts) => Value::Number(ts.0.into()), // milliseconds
        CqlValue::Inet(ip) => Value::String(ip.to_string()),
        CqlValue::Blob(b) => Value::String(general_purpose::STANDARD.encode(b)),
        CqlValue::List(vec) | CqlValue::Set(vec) => {
            Value::Array(vec.iter().map(cql_to_json).collect())
        }
        CqlValue::Map(pairs) => {
            // JSON keys must be strings. If the CQL map key isn't a string, we stringify it.
            let mut map = Map::new();
            for (k, v) in pairs {
                let key_str = match k {
                    CqlValue::Text(s) | CqlValue::Ascii(s) => s.clone(),
                    _ => format!("{:?}", k), // Fallback debug repr for non-string keys
                };
                map.insert(key_str, cql_to_json(v));
            }
            Value::Object(map)
        }
        CqlValue::UserDefinedType { fields, .. } => {
            let mut map = serde_json::Map::new();
            for (name, value_opt) in fields {
                let val = match value_opt {
                    Some(v) => cql_to_json(v), // Recursion here
                    None => Value::Null,
                };
                map.insert(name.clone(), val);
            }
            Value::Object(map)
        }
        CqlValue::Tuple(vec) => {
            let json_vec: Vec<Value> = vec
                .iter()
                .map(|opt| match opt {
                    Some(v) => cql_to_json(v),
                    None => Value::Null,
                })
                .collect();
            Value::Array(json_vec)
        }
        // Handle other types or fallbacks
        _ => Value::String(format!("{:?}", val)),
    }
}

pub(super) async fn get_partition_key_columns(
    session: std::sync::Arc<Session>,
    keyspace: &String,
    table: &String,
) -> Result<Vec<String>> {
    let query = r#"
            SELECT column_name, position
            FROM system_schema.columns
            WHERE keyspace_name = ?
            AND table_name = ?
            AND kind = 'partition_key'
            ALLOW FILTERING
        "#;
    let prepared_statement = session.prepare(query).await?;
    let result = session
        .execute_unpaged(&prepared_statement, (keyspace, table))
        .await?;
    let rows_result = result.into_rows_result()?;
    let rows_iter = rows_result.rows::<(String, i32)>()?;

    let mut pk_pairs: Vec<(i32, String)> = Vec::new();
    for row_res in rows_iter {
        let (name, pos) = row_res?;
        pk_pairs.push((pos, name));
    }

    pk_pairs.sort_by_key(|&(pos, _)| pos);

    let pk_names = pk_pairs.into_iter().map(|(_, name)| name).collect();

    Ok(pk_names)
}

// TODO: Improve this for better locality
pub(super) fn get_target_node_id(
    row: &scylla_cdc::consumer::CDCRow<'_>,
    pk_names: &Vec<String>,
    n_search_nodes: usize,
) -> usize {
    let mut hasher = XxHash64::with_seed(0);

    for col_name in pk_names {
        if let Some(val) = row.get_value(col_name) {
            // WARN: Relies on `Debug` impl for `CqlValue` and can break with changes upstream
            std::hash::Hash::hash(&val.to_string(), &mut hasher);
        }
    }

    let hash_value = hasher.finish();
    hash_value as usize % n_search_nodes
}
