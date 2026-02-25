use ::libsql as libsql_core;
use pyo3::create_exception;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyModule, PyTuple};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use tokio::sync::Mutex;
use std::time::Duration;
use pyo3_async_runtimes::tokio::future_into_py;


const LEGACY_TRANSACTION_CONTROL: i32 = -1;
const VERSION: &str = "0.1.14-stable";

enum ListOrTuple {
    List(Py<PyList>),
    Tuple(Py<PyTuple>),
}

impl<'py> FromPyObject<'py> for ListOrTuple {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(list) = ob.downcast::<PyList>() {
            Ok(ListOrTuple::List(list.clone().unbind()))
        } else if let Ok(tuple) = ob.downcast::<PyTuple>() {
            Ok(ListOrTuple::Tuple(tuple.clone().unbind()))
        } else {
            Err(PyValueError::new_err("Expected a list or tuple for parameters"))
        }
    }
}

fn to_py_err<E: std::fmt::Display>(error: E) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn is_remote_path(path: &str) -> bool {
    path.starts_with("libsql://") || path.starts_with("http://") || path.starts_with("https://")
}

#[pyfunction]
#[pyo3(signature = (database, timeout=5.0, isolation_level="DEFERRED".to_string(), _check_same_thread=true, _uri=false, sync_url=None, sync_interval=None, offline=false, auth_token=None, encryption_key=None, autocommit=LEGACY_TRANSACTION_CONTROL))]
fn connect<'py>(
    py: Python<'py>,
    database: String,
    timeout: f64,
    isolation_level: Option<String>,
    _check_same_thread: bool,
    _uri: bool,
    sync_url: Option<String>,
    sync_interval: Option<f64>,
    offline: bool,
    auth_token: Option<String>,
    encryption_key: Option<String>,
    autocommit: i32,
) -> PyResult<Bound<'py, PyAny>> {
    let auth_token = auth_token.unwrap_or_default();
    future_into_py(py, async move {
        let ver = env!("CARGO_PKG_VERSION");
        let ver = format!("libsql-python-rpc-{ver}");
        let encryption_config = match encryption_key {
            Some(key) => {
                let cipher = libsql_core::Cipher::default();
                Some(libsql_core::EncryptionConfig::new(cipher, key.into()))
            }
            None => None,
        };
        let db = if is_remote_path(&database) {
            libsql_core::Database::open_remote_internal(database, auth_token.clone(), ver).map_err(to_py_err)?
        } else {
            match sync_url {
                Some(sync_url) => {
                    let sync_interval = sync_interval.map(|i| std::time::Duration::from_secs_f64(i));
                    let mut builder = libsql_core::Builder::new_synced_database(database, sync_url, auth_token.clone());
                    if encryption_config.is_some() {
                        return Err(PyValueError::new_err("encryption is not supported for synced databases"));
                    }
                    if let Some(sync_interval) = sync_interval { builder = builder.sync_interval(sync_interval); }
                    builder = builder.remote_writes(!offline);
                    builder.build().await.map_err(to_py_err)?
                }
                None => {
                    let mut builder = libsql_core::Builder::new_local(database);
                    if let Some(config) = encryption_config { builder = builder.encryption_config(config); }
                    builder.build().await.map_err(to_py_err)?
                }
            }
        };
        let conn = db.connect().map_err(to_py_err)?;
        conn.busy_timeout(Duration::from_secs_f64(timeout)).map_err(to_py_err)?;
        let autocommit_val = if autocommit == LEGACY_TRANSACTION_CONTROL { isolation_level.is_none() as i32 } else { autocommit };
        Ok(Connection {
            db: Arc::new(db),
            conn: Arc::new(Mutex::new(Some(conn))),
            isolation_level,
            autocommit: autocommit_val,
        })
    })
}

#[pyclass]
pub struct Connection {
    db: Arc<libsql_core::Database>,
    conn: Arc<Mutex<Option<libsql_core::Connection>>>,
    isolation_level: Option<String>,
    #[pyo3(get, set)]
    autocommit: i32,
}

#[pymethods]
impl Connection {
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let conn_arc = self.conn.clone();
        future_into_py(py, async move {
            if let Some(c) = conn_arc.lock().await.take() { drop(c); }
            Ok(())
        })
    }
    fn cursor(&self) -> PyResult<Cursor> {
        Ok(Cursor {
            arraysize: 1,
            conn: self.conn.clone(),
            stmt: Arc::new(Mutex::new(None)),
            rows: Arc::new(Mutex::new(None)),
            rowcount: Arc::new(AtomicI64::new(0)),
            last_insert_rowid: Arc::new(AtomicI64::new(0)),
            isolation_level: self.isolation_level.clone(),
            autocommit: self.autocommit,
            done: Arc::new(Mutex::new(false)),
        })
    }
    fn sync<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let db = self.db.clone();
        future_into_py(py, async move {
            db.sync().await.map_err(to_py_err)?;
            Ok(())
        })
    }
    fn commit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let conn_arc = self.conn.clone();
        future_into_py(py, async move {
            let guard = conn_arc.lock().await;
            if let Some(conn) = guard.as_ref() {
                if !conn.is_autocommit() { conn.execute("COMMIT", ()).await.map_err(to_py_err)?; }
            }
            Ok(())
        })
    }
    fn rollback<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let conn_arc = self.conn.clone();
        future_into_py(py, async move {
            let guard = conn_arc.lock().await;
            if let Some(conn) = guard.as_ref() {
                if !conn.is_autocommit() { conn.execute("ROLLBACK", ()).await.map_err(to_py_err)?; }
            }
            Ok(())
        })
    }
    #[pyo3(signature = (sql, parameters=None))]
    fn execute<'py>(&self, py: Python<'py>, sql: String, parameters: Option<ListOrTuple>) -> PyResult<Bound<'py, PyAny>> {
        let cursor = self.cursor()?;
        let cursor_py = Py::new(py, cursor.clone())?;
        let params = extract_parameters(py, parameters)?;
        let (conn, stmt, rows, rc, rid, ac, isl) = (cursor.conn.clone(), cursor.stmt.clone(), cursor.rows.clone(), cursor.rowcount.clone(), cursor.last_insert_rowid.clone(), cursor.autocommit, cursor.isolation_level.clone());
        future_into_py(py, async move {
            let (changes, id) = execute_async(conn, stmt, rows, ac, isl, sql, params).await?;
            rc.store(changes, Ordering::SeqCst); rid.store(id, Ordering::SeqCst);
            Ok(cursor_py)
        })
    }
    #[pyo3(signature = (sql, parameters=None))]
    fn executemany<'py>(&self, py: Python<'py>, sql: String, parameters: Option<Bound<'py, PyList>>) -> PyResult<Bound<'py, PyAny>> {
        let cursor = self.cursor()?;
        let cursor_py = Py::new(py, cursor.clone())?;
        let mut p_list = vec![];
        if let Some(ps) = parameters {
            for p in ps.iter() { p_list.push(extract_parameters(py, Some(p.extract::<ListOrTuple>()?))?); }
        }
        let (conn, stmt, rows, rc, rid, ac, isl) = (cursor.conn.clone(), cursor.stmt.clone(), cursor.rows.clone(), cursor.rowcount.clone(), cursor.last_insert_rowid.clone(), cursor.autocommit, cursor.isolation_level.clone());
        future_into_py(py, async move {
            let (mut tc, mut lr) = (0, 0);
            for p in p_list {
                let (c, r) = execute_async(conn.clone(), stmt.clone(), rows.clone(), ac, isl.clone(), sql.clone(), p).await?;
                tc += c; lr = r;
            }
            rc.store(tc, Ordering::SeqCst); rid.store(lr, Ordering::SeqCst);
            Ok(cursor_py)
        })
    }
    fn executescript<'py>(&self, py: Python<'py>, script: String) -> PyResult<Bound<'py, PyAny>> {
        let cursor = self.cursor()?;
        let cursor_py = Py::new(py, cursor.clone())?;
        let conn_arc = cursor.conn.clone();
        future_into_py(py, async move {
            let guard = conn_arc.lock().await;
            if let Some(conn) = guard.as_ref() { conn.execute_batch(&script).await.map_err(to_py_err)?; }
            Ok(cursor_py)
        })
    }
    #[getter]
    fn isolation_level(&self) -> Option<String> { self.isolation_level.clone() }
    #[getter]
    fn in_transaction(&self) -> PyResult<bool> {
        let guard = self.conn.blocking_lock();
        if let Some(conn) = guard.as_ref() { Ok(!conn.is_autocommit() || self.autocommit == 0) } else { Ok(false) }
    }
    fn __aenter__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move { Ok(slf) })
    }
    #[pyo3(signature = (exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __aexit__<'py>(&self, py: Python<'py>, exc_type: Option<PyObject>, _exc_val: Option<PyObject>, _exc_tb: Option<PyObject>) -> PyResult<Bound<'py, PyAny>> {
        let conn_arc = self.conn.clone();
        let is_error = exc_type.is_some();
        future_into_py(py, async move {
            let guard = conn_arc.lock().await;
            if let Some(conn) = guard.as_ref() {
                if !conn.is_autocommit() {
                    if is_error { let _ = conn.execute("ROLLBACK", ()).await; } else { let _ = conn.execute("COMMIT", ()).await; }
                }
            }
            Ok(false)
        })
    }
}

#[pyclass]
#[derive(Clone)]
pub struct Cursor {
    #[pyo3(get, set)]
    arraysize: usize,
    conn: Arc<Mutex<Option<libsql_core::Connection>>>,
    stmt: Arc<Mutex<Option<libsql_core::Statement>>>,
    rows: Arc<Mutex<Option<libsql_core::Rows>>>,
    rowcount: Arc<AtomicI64>,
    last_insert_rowid: Arc<AtomicI64>,
    done: Arc<Mutex<bool>>,
    isolation_level: Option<String>,
    autocommit: i32,
}

#[pymethods]
impl Cursor {
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let (c, s, r) = (self.conn.clone(), self.stmt.clone(), self.rows.clone());
        future_into_py(py, async move {
            c.lock().await.take(); s.lock().await.take(); r.lock().await.take();
            Ok(())
        })
    }
    #[pyo3(signature = (sql, parameters=None))]
    fn execute<'py>(slf: Py<Self>, py: Python<'py>, sql: String, parameters: Option<ListOrTuple>) -> PyResult<Bound<'py, PyAny>> {
        let params = extract_parameters(py, parameters)?;
        let (conn, stmt, rows, rc, rid, ac, isl) = {
            let b = slf.borrow(py);
            (b.conn.clone(), b.stmt.clone(), b.rows.clone(), b.rowcount.clone(), b.last_insert_rowid.clone(), b.autocommit, b.isolation_level.clone())
        };
        future_into_py(py, async move {
            let (c, r) = execute_async(conn, stmt, rows, ac, isl, sql, params).await?;
            rc.store(c, Ordering::SeqCst); rid.store(r, Ordering::SeqCst);
            Ok(slf)
        })
    }
    #[pyo3(signature = (sql, parameters=None))]
    fn executemany<'py>(slf: Py<Self>, py: Python<'py>, sql: String, parameters: Option<Bound<'py, PyList>>) -> PyResult<Bound<'py, PyAny>> {
        let mut p_list = vec![];
        if let Some(ps) = parameters {
            for p in ps.iter() { p_list.push(extract_parameters(py, Some(p.extract::<ListOrTuple>()?))?); }
        }
        let (conn, stmt, rows, rc, rid, ac, isl) = {
            let b = slf.borrow(py);
            (b.conn.clone(), b.stmt.clone(), b.rows.clone(), b.rowcount.clone(), b.last_insert_rowid.clone(), b.autocommit, b.isolation_level.clone())
        };
        future_into_py(py, async move {
            let (mut tc, mut lr) = (0, 0);
            for p in p_list {
                let (c, r) = execute_async(conn.clone(), stmt.clone(), rows.clone(), ac, isl.clone(), sql.clone(), p).await?;
                tc += c; lr = r;
            }
            rc.store(tc, Ordering::SeqCst); rid.store(lr, Ordering::SeqCst);
            Ok(slf)
        })
    }
    fn executescript<'py>(slf: Py<Self>, py: Python<'py>, script: String) -> PyResult<Bound<'py, PyAny>> {
        let conn_arc = slf.borrow(py).conn.clone();
        future_into_py(py, async move {
            let guard = conn_arc.lock().await;
            if let Some(conn) = guard.as_ref() { conn.execute_batch(&script).await.map_err(to_py_err)?; }
            Ok(slf)
        })
    }
    #[getter]
    fn description(&self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        let guard = self.stmt.blocking_lock();
        if let Some(stmt) = guard.as_ref() {
            let mut elements: Vec<PyObject> = vec![];
            for c in stmt.columns() {
                let e = (c.name(), py.None(), py.None(), py.None(), py.None(), py.None(), py.None()).into_pyobject(py)?.into_any().unbind();
                elements.push(e);
            }
            Ok(Some(PyTuple::new(py, elements)?.unbind().into()))
        } else { Ok(None) }
    }
    fn fetchone<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rows_arc = self.rows.clone();
        future_into_py(py, async move {
            let mut guard = rows_arc.lock().await;
            if let Some(rows) = guard.as_mut() {
                if let Some(r) = rows.next().await.map_err(to_py_err)? {
                    let cc = rows.column_count();
                    let mut vals = Vec::with_capacity(cc as usize);
                    for i in 0..cc { vals.push(r.get_value(i).map_err(to_py_err)?); }
                    drop(guard);
                    return Python::with_gil(|py| {
                        let mut py_vals = vec![];
                        for v in vals { py_vals.push(convert_value(py, v)?); }
                        Ok(PyTuple::new(py, py_vals)?.unbind().into_any())
                    });
                }
            }
            Python::with_gil(|py| Ok(py.None()))
        })
    }
    #[pyo3(signature = (size=None))]
    fn fetchmany<'py>(&self, py: Python<'py>, size: Option<usize>) -> PyResult<Bound<'py, PyAny>> {
        let (rows_arc, done_arc, arraysize) = (self.rows.clone(), self.done.clone(), self.arraysize);
        future_into_py(py, async move {
            let size = size.unwrap_or(arraysize);
            let mut guard = rows_arc.lock().await;
            let mut data = vec![];
            if let Some(rows) = guard.as_mut() {
                if !*done_arc.lock().await {
                    let cc = rows.column_count();
                    for _ in 0..size {
                        match rows.next().await.map_err(to_py_err)? {
                            Some(r) => {
                                let mut row = Vec::with_capacity(cc as usize);
                                for i in 0..cc { row.push(r.get_value(i).map_err(to_py_err)?); }
                                data.push(row);
                            }
                            None => { *done_arc.lock().await = true; break; }
                        }
                    }
                }
            }
            drop(guard);
            Python::with_gil(|py| {
                let mut elements = vec![];
                for row in data {
                    let mut py_row = vec![];
                    for v in row { py_row.push(convert_value(py, v)?); }
                    elements.push(PyTuple::new(py, py_row)?.unbind().into_any());
                }
                Ok(PyList::new(py, elements)?.unbind().into_any())
            })
        })
    }
    fn fetchall<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rows_arc = self.rows.clone();
        future_into_py(py, async move {
            let mut guard = rows_arc.lock().await;
            let mut data = vec![];
            if let Some(rows) = guard.as_mut() {
                let cc = rows.column_count();
                while let Some(r) = rows.next().await.map_err(to_py_err)? {
                    let mut row = Vec::with_capacity(cc as usize);
                    for i in 0..cc { row.push(r.get_value(i).map_err(to_py_err)?); }
                    data.push(row);
                }
            }
            drop(guard);
            Python::with_gil(|py| {
                let mut elements = vec![];
                for row in data {
                    let mut py_row = vec![];
                    for v in row { py_row.push(convert_value(py, v)?); }
                    elements.push(PyTuple::new(py, py_row)?.unbind().into_any());
                }
                Ok(PyList::new(py, elements)?.unbind().into_any())
            })
        })
    }
    #[getter]
    fn lastrowid(&self) -> PyResult<i64> { Ok(self.last_insert_rowid.load(Ordering::SeqCst)) }
    #[getter]
    fn rowcount(&self) -> PyResult<i64> { Ok(self.rowcount.load(Ordering::SeqCst)) }
}

async fn begin_transaction(conn: &libsql_core::Connection) -> PyResult<()> {
    conn.execute("BEGIN", ()).await.map_err(to_py_err)?;
    Ok(())
}
fn determine_autocommit(autocommit: i32, isolation_level: &Option<String>) -> bool {
    match autocommit { LEGACY_TRANSACTION_CONTROL => isolation_level.is_none(), _ => autocommit != 0 }
}
fn stmt_is_dml(sql: &str) -> bool {
    let s = sql.trim().to_uppercase();
    s.starts_with("INSERT") || s.starts_with("UPDATE") || s.starts_with("DELETE") || s.starts_with("REPLACE")
}
async fn execute_async(conn_arc: Arc<Mutex<Option<libsql_core::Connection>>>, stmt_arc: Arc<Mutex<Option<libsql_core::Statement>>>, rows_arc: Arc<Mutex<Option<libsql_core::Rows>>>, autocommit: i32, isolation_level: Option<String>, sql: String, params: libsql_core::params::Params) -> PyResult<(i64, i64)> {
    let mut guard = conn_arc.lock().await;
    let conn = guard.as_mut().ok_or_else(|| PyValueError::new_err("Connection closed"))?;
    if !determine_autocommit(autocommit, &isolation_level) && stmt_is_dml(&sql) && conn.is_autocommit() { begin_transaction(conn).await?; }
    let stmt = conn.prepare(&sql).await.map_err(to_py_err)?;
    let col_count = stmt.column_count();
    let mut s_guard = stmt_arc.lock().await;
    *s_guard = Some(stmt);
    let s_ref = s_guard.as_ref().unwrap();
    let mut r_guard = rows_arc.lock().await;
    if col_count > 0 { *r_guard = Some(s_ref.query(params).await.map_err(to_py_err)?); } else { s_ref.execute(params).await.map_err(to_py_err)?; *r_guard = None; }
    Ok((conn.changes() as i64, conn.last_insert_rowid()))
}

fn extract_parameters(py: Python, parameters: Option<ListOrTuple>) -> PyResult<libsql_core::params::Params> {
    match parameters {
        Some(p) => {
            let mut params = vec![];
            let (len, binder) = match &p { ListOrTuple::List(l) => (l.bind(py).len(), l.bind(py).as_any()), ListOrTuple::Tuple(t) => (t.bind(py).len(), t.bind(py).as_any()) };
            for i in 0..len {
                let item = if let Ok(l) = binder.downcast::<PyList>() { l.get_item(i)? } else { binder.downcast::<PyTuple>().unwrap().get_item(i)? };
                let val = if item.is_none() { libsql_core::Value::Null }
                else if let Ok(v) = item.extract::<i64>() { libsql_core::Value::Integer(v) }
                else if let Ok(s) = item.extract::<String>() { libsql_core::Value::Text(s) }
                else if let Ok(v) = item.extract::<f64>() { libsql_core::Value::Real(v) }
                else if let Ok(v) = item.extract::<Vec<u8>>() { libsql_core::Value::Blob(v) }
                else { libsql_core::Value::Null };
                params.push(val);
            }
            Ok(libsql_core::params::Params::Positional(params))
        }
        None => Ok(libsql_core::params::Params::None),
    }
}

fn convert_value(py: Python<'_>, value: libsql_core::Value) -> PyResult<PyObject> {
    match value {
        libsql_core::Value::Null => Ok(py.None()),
        libsql_core::Value::Integer(v) => Ok(v.into_pyobject(py)?.unbind().into_any()),
        libsql_core::Value::Real(v) => Ok(v.into_pyobject(py)?.unbind().into_any()),
        libsql_core::Value::Text(v) => Ok(v.into_pyobject(py)?.unbind().into_any()),
        libsql_core::Value::Blob(v) => Ok(v.as_slice().into_pyobject(py)?.unbind().into_any()),
    }
}

create_exception!(aiolibsql, Error, pyo3::exceptions::PyException);
#[pymodule]
fn aiolibsql(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("VERSION", VERSION)?;
    m.add("LEGACY_TRANSACTION_CONTROL", LEGACY_TRANSACTION_CONTROL)?;
    m.add("paramstyle", "qmark")?;
    m.add("sqlite_version_info", (3, 42, 0))?;
    m.add("Error", py.get_type::<Error>())?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_class::<Connection>()?;
    m.add_class::<Cursor>()?;
    Ok(())
}
