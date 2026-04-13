use ::libsql as libsql_core;
use parking_lot::Mutex as SyncMutex;
use pyo3::{
    create_exception,
    exceptions::PyValueError,
    prelude::*,
    types::{
        PyAny,
        PyBytes,
        PyFloat,
        PyInt,
        PyList,
        PyModule,
        PyString,
        PyTuple,
    },
};
use pyo3_async_runtimes::tokio::future_into_py;
use std::{
    sync::{
        atomic::{
            AtomicBool,
            AtomicI64,
            AtomicUsize,
            Ordering,
        },
        Arc,
    },
    time::Duration,
};
use tokio::sync::{
    Mutex as AsyncMutex,
    Semaphore,
};

const LEGACY_TRANSACTION_CONTROL: i32 = -1;
const VERSION: &str = "0.2.8";

create_exception!(
    aiolibsql,
    DatabaseError,
    pyo3::exceptions::PyException
);
create_exception!(
    aiolibsql,
    OperationalError,
    DatabaseError
);
create_exception!(
    aiolibsql,
    IntegrityError,
    DatabaseError
);
create_exception!(
    aiolibsql,
    TimeoutError,
    DatabaseError
);

fn to_py_err<E: std::fmt::Display>(error: E) -> PyErr {
    let msg = error.to_string();
    let lower = msg.to_lowercase();
    if lower.contains("constraint") || lower.contains("unique") || lower.contains("foreign key") {
        IntegrityError::new_err(msg)
    } else if lower.contains("timeout") || lower.contains("busy") || lower.contains("locked") {
        TimeoutError::new_err(msg)
    } else if lower.contains("syntax") || lower.contains("no such table") || lower.contains("unrecognized") {
        OperationalError::new_err(msg)
    } else {
        DatabaseError::new_err(msg)
    }
}

fn is_remote_path(path: &str) -> bool {
    path.starts_with("libsql://")
        || path.starts_with("http://")
        || path.starts_with("https://")
}

fn extract_parameter(_py: Python, item: &Bound<'_, PyAny>) -> PyResult<libsql_core::Value> {
    if item.is_none() {
        Ok(libsql_core::Value::Null)
    } else if item.is_instance_of::<PyInt>() {
        Ok(libsql_core::Value::Integer(item.extract::<i64>()?))
    } else if item.is_instance_of::<PyString>() {
        Ok(libsql_core::Value::Text(item.extract::<String>()?))
    } else if item.is_instance_of::<PyFloat>() {
        Ok(libsql_core::Value::Real(item.extract::<f64>()?))
    } else if item.is_instance_of::<PyBytes>() {
        Ok(libsql_core::Value::Blob(item.extract::<Vec<u8>>()?))
    } else if item.is_instance_of::<pyo3::types::PyByteArray>() {
        Ok(libsql_core::Value::Blob(item.extract::<Vec<u8>>()?))
    } else if item.is_instance_of::<pyo3::types::PyBool>() {
        let v: bool = item.extract()?;
        Ok(libsql_core::Value::Integer(if v { 1 } else { 0 }))
    } else {
        Ok(libsql_core::Value::Null)
    }
}

enum ListOrTuple {
    List(Py<PyList>),
    Tuple(Py<PyTuple>),
}

impl<'py> FromPyObject<'py> for ListOrTuple {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if ob.is_instance_of::<PyList>() {
            Ok(ListOrTuple::List(ob.downcast::<PyList>()?.clone().unbind()))
        } else if ob.is_instance_of::<PyTuple>() {
            Ok(ListOrTuple::Tuple(ob.downcast::<PyTuple>()?.clone().unbind()))
        } else {
            Err(PyValueError::new_err("expected list or tuple"))
        }
    }
}

fn extract_parameters(
    py: Python,
    parameters: Option<ListOrTuple>,
) -> PyResult<libsql_core::params::Params> {
    match parameters {
        Some(p) => {
            let mut params = Vec::new();
            let (len, binder) = match &p {
                ListOrTuple::List(l) => {
                    let b = l.bind(py);
                    (b.len(), b.as_any())
                }
                ListOrTuple::Tuple(t) => {
                    let b = t.bind(py);
                    (b.len(), b.as_any())
                }
            };
            for i in 0..len {
                let item = if let Ok(l) = binder.downcast::<PyList>() {
                    l.get_item(i)?
                } else {
                    binder.downcast::<PyTuple>().unwrap().get_item(i)?
                };
                params.push(extract_parameter(py, &item)?);
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
        libsql_core::Value::Blob(v) => Ok(PyBytes::new(py, &v).into_pyobject(py)?.unbind().into_any()),
    }
}

async fn begin_transaction(conn: &libsql_core::Connection) -> PyResult<()> {
    conn.execute("BEGIN", ()).await.map_err(to_py_err)?;
    Ok(())
}

fn determine_autocommit(autocommit: i32, isolation_level: &Option<String>) -> bool {
    match autocommit {
        LEGACY_TRANSACTION_CONTROL => isolation_level.is_none(),
        _ => autocommit != 0,
    }
}

fn stmt_is_dml(sql: &str) -> bool {
    let s = sql.trim_start().to_uppercase();
    s.starts_with("INSERT")
        || s.starts_with("UPDATE")
        || s.starts_with("DELETE")
        || s.starts_with("REPLACE")
        || s.starts_with("CREATE")
        || s.starts_with("DROP")
        || s.starts_with("ALTER")
        || s.starts_with("ATTACH")
        || s.starts_with("DETACH")
        || s.starts_with("VACUUM")
}

fn stmt_is_read(sql: &str) -> bool {
    !stmt_is_dml(sql)
}

struct TxGuard {
    conn: libsql_core::Connection,
    started: bool,
    done: bool,
}

impl TxGuard {
    async fn new(
        conn: libsql_core::Connection,
        is_tx: bool,
    ) -> PyResult<Self> {
        let started = if is_tx && conn.is_autocommit() {
            begin_transaction(&conn).await?;
            true
        } else {
            false
        };
        Ok(Self {
            conn,
            started,
            done: false,
        })
    }

    async fn commit(mut self) -> PyResult<()> {
        if self.started && !self.done {
            self.conn.execute("COMMIT", ()).await.map_err(to_py_err)?;
            self.done = true;
        }
        Ok(())
    }

    async fn rollback(mut self) -> PyResult<()> {
        if self.started && !self.done {
            let _ = self.conn.execute("ROLLBACK", ()).await;
            self.done = true;
        }
        Ok(())
    }
    
    fn conn(&self) -> &libsql_core::Connection {
        &self.conn
    }
}

#[pyfunction]
#[pyo3(
    signature = (
        database,
        timeout=5.0,
        isolation_level="DEFERRED".to_string(),
        _check_same_thread=true,
        _uri=false,
        sync_url=None,
        sync_interval=None,
        offline=false,
        auth_token=None,
        encryption_key=None,
        autocommit=LEGACY_TRANSACTION_CONTROL
    )
)]
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
        let ver = "libsql-python-rpc-0.2.8";
        let encryption_config = match encryption_key {
            Some(key) => {
                let cipher = libsql_core::Cipher::default();
                Some(libsql_core::EncryptionConfig::new(cipher, key.into()))
            }
            None => None,
        };
        let db = if is_remote_path(&database) {
            libsql_core::Database::open_remote_internal(
                database,
                auth_token.clone(),
                ver.to_string(),
            )
            .map_err(to_py_err)?
        } else {
            match sync_url {
                Some(sync_url) => {
                    let sync_interval = sync_interval.map(Duration::from_secs_f64);
                    let mut builder = libsql_core::Builder::new_synced_database(
                        database,
                        sync_url,
                        auth_token.clone(),
                    );
                    if encryption_config.is_some() {
                        return Err(PyValueError::new_err("no encryption sync"));
                    }
                    if let Some(sync_interval) = sync_interval {
                        builder = builder.sync_interval(sync_interval);
                    }
                    builder = builder.remote_writes(!offline);
                    builder.build().await.map_err(to_py_err)?
                }
                None => {
                    let mut builder = libsql_core::Builder::new_local(database);
                    if let Some(config) = encryption_config {
                        builder = builder.encryption_config(config);
                    }
                    builder.build().await.map_err(to_py_err)?
                }
            }
        };
        let conn = db.connect().map_err(to_py_err)?;
        conn.busy_timeout(Duration::from_secs_f64(timeout)).map_err(to_py_err)?;
        let autocommit_val = if autocommit == LEGACY_TRANSACTION_CONTROL {
            if isolation_level.is_none() { 1 } else { 0 }
        } else {
            autocommit
        };
        Ok(Connection {
            db: Arc::new(db),
            conn: Arc::new(SyncMutex::new(Some(conn))),
            isolation_level,
            autocommit: autocommit_val,
        })
    })
}

#[pyclass]
pub struct Connection {
    db: Arc<libsql_core::Database>,
    conn: Arc<SyncMutex<Option<libsql_core::Connection>>>,
    isolation_level: Option<String>,
    #[pyo3(get, set)]
    autocommit: i32,
}

#[pymethods]
impl Connection {
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let conn_arc = self.conn.clone();
        future_into_py(py, async move {
            let lock = conn_arc.lock().take();
            drop(lock);
            Ok(())
        })
    }

    fn cursor(&self) -> PyResult<Cursor> {
        Ok(Cursor {
            arraysize: 1,
            conn: self.conn.clone(),
            rows: Arc::new(AsyncMutex::new(None)),
            columns: Arc::new(SyncMutex::new(None)),
            rowcount: Arc::new(AtomicI64::new(0)),
            last_insert_rowid: Arc::new(AtomicI64::new(0)),
            isolation_level: self.isolation_level.clone(),
            autocommit: self.autocommit,
            done: Arc::new(AtomicBool::new(false)),
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
            let conn_opt = {
                let guard = conn_arc.lock();
                guard.as_ref().cloned()
            };
            if let Some(conn) = conn_opt {
                if !conn.is_autocommit() {
                    conn.execute("COMMIT", ()).await.map_err(to_py_err)?;
                }
            }
            Ok(())
        })
    }

    fn rollback<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let conn_arc = self.conn.clone();
        future_into_py(py, async move {
            let conn_opt = {
                let guard = conn_arc.lock();
                guard.as_ref().cloned()
            };
            if let Some(conn) = conn_opt {
                if !conn.is_autocommit() {
                    conn.execute("ROLLBACK", ()).await.map_err(to_py_err)?;
                }
            }
            Ok(())
        })
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        parameters: Option<ListOrTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let cursor = self.cursor()?;
        let py_cursor = cursor.into_pyobject(py)?.unbind();
        Cursor::execute(py_cursor, py, sql, parameters)
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn executemany<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        parameters: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let cursor = self.cursor()?;
        let py_cursor = cursor.into_pyobject(py)?.unbind();
        Cursor::executemany(py_cursor, py, sql, parameters)
    }

    fn executescript<'py>(
        &self,
        py: Python<'py>,
        script: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let cursor = self.cursor()?;
        let py_cursor = cursor.into_pyobject(py)?.unbind();
        Cursor::executescript(py_cursor, py, script)
    }

    #[getter]
    fn isolation_level(&self) -> Option<String> {
        self.isolation_level.clone()
    }

    #[getter]
    fn in_transaction(&self) -> PyResult<bool> {
        let guard = self.conn.lock();
        if let Some(conn) = guard.as_ref() {
            Ok(!conn.is_autocommit() || self.autocommit == 0)
        } else {
            Ok(false)
        }
    }

    fn __aenter__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move { Ok(slf) })
    }

    #[pyo3(signature = (exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        exc_type: Option<PyObject>,
        _exc_val: Option<PyObject>,
        _exc_tb: Option<PyObject>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let conn_arc = self.conn.clone();
        let is_error = exc_type.is_some();
        future_into_py(py, async move {
            let conn_opt = {
                let guard = conn_arc.lock();
                guard.as_ref().cloned()
            };
            if let Some(conn) = conn_opt {
                if !conn.is_autocommit() {
                    if is_error {
                        let _ = conn.execute("ROLLBACK", ()).await;
                    } else {
                        let _ = conn.execute("COMMIT", ()).await;
                    }
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
    conn: Arc<SyncMutex<Option<libsql_core::Connection>>>,
    rows: Arc<AsyncMutex<Option<libsql_core::Rows>>>,
    columns: Arc<SyncMutex<Option<Vec<String>>>>,
    rowcount: Arc<AtomicI64>,
    last_insert_rowid: Arc<AtomicI64>,
    done: Arc<AtomicBool>,
    isolation_level: Option<String>,
    autocommit: i32,
}

#[pymethods]
impl Cursor {
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let r = self.rows.clone();
        future_into_py(py, async move {
            r.lock().await.take();
            Ok(())
        })
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn execute<'py>(
        slf: Py<Self>,
        py: Python<'py>,
        sql: String,
        parameters: Option<ListOrTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let params = extract_parameters(py, parameters)?;
        let (conn, rows, cols, rc, rid, ac, isl, dn) = {
            let b = slf.borrow(py);
            (
                b.conn.clone(),
                b.rows.clone(),
                b.columns.clone(),
                b.rowcount.clone(),
                b.last_insert_rowid.clone(),
                b.autocommit,
                b.isolation_level.clone(),
                b.done.clone(),
            )
        };
        future_into_py(py, async move {
            let conn_opt = {
                let guard = conn.lock();
                guard.as_ref().cloned()
            };
            if let Some(c) = conn_opt {
                let is_tx = (!determine_autocommit(ac, &isl)) && stmt_is_dml(&sql);
                let guard = TxGuard::new(c, is_tx).await?;
                
                let stmt_res = guard.conn().prepare(&sql).await;
                if let Err(e) = stmt_res {
                    guard.rollback().await?;
                    return Err(to_py_err(e));
                }
                let stmt = stmt_res.unwrap();
                let col_count = stmt.column_count();
                
                let mut c_names = Vec::new();
                for c in stmt.columns() {
                    c_names.push(c.name().to_string());
                }
                *cols.lock() = if col_count > 0 { Some(c_names) } else { None };
                
                if col_count > 0 {
                    let rs = match stmt.query(params).await {
                        Ok(r) => r,
                        Err(e) => {
                            guard.rollback().await?;
                            return Err(to_py_err(e));
                        }
                    };
                    *rows.lock().await = Some(rs);
                    dn.store(false, Ordering::SeqCst);
                } else {
                    if let Err(e) = stmt.execute(params).await {
                        guard.rollback().await?;
                        return Err(to_py_err(e));
                    }
                    *rows.lock().await = None;
                    dn.store(true, Ordering::SeqCst);
                }
                
                rc.store(guard.conn().changes() as i64, Ordering::SeqCst);
                rid.store(guard.conn().last_insert_rowid(), Ordering::SeqCst);
                guard.commit().await?;
            }
            Ok(slf)
        })
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn executemany<'py>(
        slf: Py<Self>,
        py: Python<'py>,
        sql: String,
        parameters: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (conn, rows, cols, rc, rid, ac, isl, dn) = {
            let b = slf.borrow(py);
            (
                b.conn.clone(),
                b.rows.clone(),
                b.columns.clone(),
                b.rowcount.clone(),
                b.last_insert_rowid.clone(),
                b.autocommit,
                b.isolation_level.clone(),
                b.done.clone(),
            )
        };
        
        let parameters_iter: Option<PyObject> = parameters.map(|p| {
            p.try_iter().unwrap().into_pyobject(p.py()).unwrap().into_any().unbind()
        });
        
        future_into_py(py, async move {
            let conn_opt = {
                let guard = conn.lock();
                guard.as_ref().cloned()
            };
            if let Some(c) = conn_opt {
                *rows.lock().await = None;
                *cols.lock() = None;
                dn.store(true, Ordering::SeqCst);
                
                let is_tx = (!determine_autocommit(ac, &isl)) && stmt_is_dml(&sql);
                let txguard = TxGuard::new(c, is_tx).await?;
                
                let stmt_res = txguard.conn().prepare(&sql).await;
                if let Err(e) = stmt_res {
                    txguard.rollback().await?;
                    return Err(to_py_err(e));
                }
                let stmt = stmt_res.unwrap();
                let mut total_changes = 0;
                let mut last_id = 0;
                
                if let Some(iterator_obj) = parameters_iter {
                    loop {
                        let chunk_res: PyResult<Vec<libsql_core::params::Params>> = Python::with_gil(|py| {
                            let mut chunk = Vec::with_capacity(100);
                            let iter = iterator_obj.bind(py);
                            for _ in 0..100 {
                                match iter.call_method0("__next__") {
                                    Ok(item) => {
                                        chunk.push(extract_parameters(py, Some(ListOrTuple::extract_bound(&item)?))?);
                                    }
                                    Err(e) => {
                                        if e.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) {
                                            break;
                                        }
                                        return Err(e);
                                    }
                                }
                            }
                            Ok(chunk)
                        });
                        
                        let chunk = match chunk_res {
                            Ok(c) => c,
                            Err(e) => {
                                txguard.rollback().await?;
                                return Err(e);
                            }
                        };
                        
                        if chunk.is_empty() { break; }
                        for p in chunk {
                            if let Err(e) = stmt.execute(p).await {
                                txguard.rollback().await?;
                                return Err(to_py_err(e));
                            }
                            total_changes += txguard.conn().changes() as i64;
                            last_id = txguard.conn().last_insert_rowid();
                        }
                    }
                }
                rc.store(total_changes, Ordering::SeqCst);
                rid.store(last_id, Ordering::SeqCst);
                txguard.commit().await?;
            }
            Ok(slf)
        })
    }

    fn executescript<'py>(
        slf: Py<Self>,
        py: Python<'py>,
        script: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let conn_arc = slf.borrow(py).conn.clone();
        future_into_py(py, async move {
            let conn_opt = {
                let guard = conn_arc.lock();
                guard.as_ref().cloned()
            };
            if let Some(conn) = conn_opt {
                conn.execute_batch(&script).await.map_err(to_py_err)?;
            }
            Ok(slf)
        })
    }

    #[getter]
    fn description(&self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        let guard = self.columns.lock();
        if let Some(cols) = guard.as_ref() {
            let mut elements = Vec::new();
            for name in cols {
                let e = (
                    name.clone(),
                    py.None(),
                    py.None(),
                    py.None(),
                    py.None(),
                    py.None(),
                    py.None(),
                )
                    .into_pyobject(py)?
                    .into_any()
                    .unbind();
                elements.push(e);
            }
            Ok(Some(PyTuple::new(py, elements)?.unbind().into()))
        } else {
            Ok(None)
        }
    }

    fn fetchone<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rows_arc = self.rows.clone();
        future_into_py(py, async move {
            let mut guard = rows_arc.lock().await;
            if let Some(rows) = guard.as_mut() {
                if let Some(r) = rows.next().await.map_err(to_py_err)? {
                    let cc = rows.column_count();
                    let mut vals = Vec::with_capacity(cc as usize);
                    for i in 0..cc {
                        vals.push(r.get_value(i).map_err(to_py_err)?);
                    }
                    drop(guard);
                    return Python::with_gil(|py| {
                        let mut py_vals = Vec::with_capacity(cc as usize);
                        for v in vals {
                            py_vals.push(convert_value(py, v)?);
                        }
                        Ok(PyTuple::new(py, py_vals)?.unbind().into_any())
                    });
                }
            }
            Python::with_gil(|py| Ok(py.None()))
        })
    }

    #[pyo3(signature = (size=None))]
    fn fetchmany<'py>(
        &self,
        py: Python<'py>,
        size: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rows_arc = self.rows.clone();
        let done_arc = self.done.clone();
        let arraysize = self.arraysize;
        future_into_py(py, async move {
            let size = size.unwrap_or(arraysize);
            let mut guard = rows_arc.lock().await;
            let mut data = Vec::new();
            if let Some(rows) = guard.as_mut() {
                if !done_arc.load(Ordering::SeqCst) {
                    let cc = rows.column_count();
                    for _ in 0..size {
                        match rows.next().await.map_err(to_py_err)? {
                            Some(r) => {
                                let mut row = Vec::with_capacity(cc as usize);
                                for i in 0..cc {
                                    row.push(r.get_value(i).map_err(to_py_err)?);
                                }
                                data.push(row);
                            }
                            None => {
                                done_arc.store(true, Ordering::SeqCst);
                                break;
                            }
                        }
                    }
                }
            }
            drop(guard);
            Python::with_gil(|py| {
                let mut elements = Vec::with_capacity(data.len());
                for row in data {
                    let mut py_row = Vec::with_capacity(row.len());
                    for v in row {
                        py_row.push(convert_value(py, v)?);
                    }
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
            let mut data = Vec::new();
            if let Some(rows) = guard.as_mut() {
                let cc = rows.column_count();
                while let Some(r) = rows.next().await.map_err(to_py_err)? {
                    let mut row = Vec::with_capacity(cc as usize);
                    for i in 0..cc {
                        row.push(r.get_value(i).map_err(to_py_err)?);
                    }
                    data.push(row);
                }
            }
            drop(guard);
            Python::with_gil(|py| {
                let mut elements = Vec::with_capacity(data.len());
                for row in data {
                    let mut py_row = Vec::with_capacity(row.len());
                    for v in row {
                        py_row.push(convert_value(py, v)?);
                    }
                    elements.push(PyTuple::new(py, py_row)?.unbind().into_any());
                }
                Ok(PyList::new(py, elements)?.unbind().into_any())
            })
        })
    }

    #[getter]
    fn lastrowid(&self) -> i64 {
        self.last_insert_rowid.load(Ordering::SeqCst)
    }

    #[getter]
    fn rowcount(&self) -> i64 {
        self.rowcount.load(Ordering::SeqCst)
    }

    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let rows_arc = self.rows.clone();
        future_into_py(py, async move {
            let mut guard = rows_arc.lock().await;
            if let Some(rows) = guard.as_mut() {
                if let Some(r) = rows.next().await.map_err(to_py_err)? {
                    let cc = rows.column_count();
                    let mut vals = Vec::with_capacity(cc as usize);
                    for i in 0..cc {
                        vals.push(r.get_value(i).map_err(to_py_err)?);
                    }
                    drop(guard);
                    return Python::with_gil(|py| {
                        let mut py_vals = Vec::with_capacity(cc as usize);
                        for v in vals {
                            py_vals.push(convert_value(py, v)?);
                        }
                        PyTuple::new(py, py_vals).map(|t| t.into_any().unbind())
                    });
                }
            }
            Err(pyo3::exceptions::PyStopAsyncIteration::new_err("done"))
        }).map(Some)
    }
}

#[pyclass]
pub struct ConnectionPool {
    #[allow(dead_code)]
    db: Arc<libsql_core::Database>,
    writer: Arc<SyncMutex<Option<libsql_core::Connection>>>,
    readers: Vec<Arc<SyncMutex<Option<libsql_core::Connection>>>>,
    reader_idx: Arc<AtomicUsize>,
    reader_sem: Arc<Semaphore>,
    writer_sem: Arc<Semaphore>,
    pool_size: usize,
}

impl ConnectionPool {
    fn _get_reader(&self) -> Arc<SyncMutex<Option<libsql_core::Connection>>> {
        for r in &self.readers {
            if let Some(guard) = r.try_lock() {
                if guard.is_some() {
                    return r.clone();
                }
            }
        }
        let idx = self.reader_idx.fetch_add(1, Ordering::Relaxed) % self.readers.len();
        self.readers[idx].clone()
    }
}

#[pymethods]
impl ConnectionPool {
    #[getter]
    fn size(&self) -> usize {
        self.pool_size
    }

    #[getter]
    fn reader_count(&self) -> usize {
        self.readers.len()
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        parameters: Option<ListOrTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let params = extract_parameters(py, parameters)?;
        let is_read = stmt_is_read(&sql);

        if is_read {
            let reader_arc = self._get_reader();
            let sem = self.reader_sem.clone();
            future_into_py(py, async move {
                let _permit = sem.acquire().await.map_err(|e| PyValueError::new_err(e.to_string()))?;
                let c_opt = { let guard = reader_arc.lock(); guard.as_ref().cloned() };
                
                if let Some(conn) = c_opt {
                    let stmt = conn.prepare(&sql).await.map_err(to_py_err)?;
                    let col_count = stmt.column_count();
                    let mut desc_cols = Vec::new();
                    for c in stmt.columns() {
                        desc_cols.push(c.name().to_string());
                    }
                    if col_count > 0 {
                        let rows = stmt.query(params).await.map_err(to_py_err)?;
                        let desc = if !desc_cols.is_empty() { Some(desc_cols) } else { None };
                        Ok(PoolCursor {
                            rows: Arc::new(AsyncMutex::new(Some(rows))),
                            columns: Arc::new(SyncMutex::new(desc)),
                            rowcount: conn.changes() as i64,
                            lastrowid: conn.last_insert_rowid(),
                            done: Arc::new(AtomicBool::new(false)),
                            arraysize: 1,
                        })
                    } else {
                        stmt.execute(params).await.map_err(to_py_err)?;
                        Ok(PoolCursor {
                            rows: Arc::new(AsyncMutex::new(None)),
                            columns: Arc::new(SyncMutex::new(None)),
                            rowcount: conn.changes() as i64,
                            lastrowid: conn.last_insert_rowid(),
                            done: Arc::new(AtomicBool::new(true)),
                            arraysize: 1,
                        })
                    }
                } else {
                    Err(PyValueError::new_err("closed"))
                }
            })
        } else {
            let writer_arc = self.writer.clone();
            let sem = self.writer_sem.clone();
            future_into_py(py, async move {
                let _permit = sem.acquire().await.map_err(|e| PyValueError::new_err(e.to_string()))?;
                let c_opt = { let guard = writer_arc.lock(); guard.as_ref().cloned() };
                
                if let Some(conn) = c_opt {
                    let stmt = conn.prepare(&sql).await.map_err(to_py_err)?;
                    let col_count = stmt.column_count();
                    let mut desc_cols = Vec::new();
                    for c in stmt.columns() {
                        desc_cols.push(c.name().to_string());
                    }
                    if col_count > 0 {
                        let rows = stmt.query(params).await.map_err(to_py_err)?;
                        let desc = if !desc_cols.is_empty() { Some(desc_cols) } else { None };
                        Ok(PoolCursor {
                            rows: Arc::new(AsyncMutex::new(Some(rows))),
                            columns: Arc::new(SyncMutex::new(desc)),
                            rowcount: conn.changes() as i64,
                            lastrowid: conn.last_insert_rowid(),
                            done: Arc::new(AtomicBool::new(false)),
                            arraysize: 1,
                        })
                    } else {
                        stmt.execute(params).await.map_err(to_py_err)?;
                        Ok(PoolCursor {
                            rows: Arc::new(AsyncMutex::new(None)),
                            columns: Arc::new(SyncMutex::new(None)),
                            rowcount: conn.changes() as i64,
                            lastrowid: conn.last_insert_rowid(),
                            done: Arc::new(AtomicBool::new(true)),
                            arraysize: 1,
                        })
                    }
                } else {
                    Err(PyValueError::new_err("closed"))
                }
            })
        }
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn executemany<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        parameters: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let parameters_iter: Option<PyObject> = parameters.map(|p| {
            p.try_iter().unwrap().into_pyobject(p.py()).unwrap().into_any().unbind()
        });
        
        let writer_arc = self.writer.clone();
        let sem = self.writer_sem.clone();
        
        future_into_py(py, async move {
            let _permit = sem.acquire().await.map_err(|e| PyValueError::new_err(e.to_string()))?;
            let conn_opt = { let guard = writer_arc.lock(); guard.as_ref().cloned() };
            if let Some(conn) = conn_opt {
                let txguard = TxGuard::new(conn, true).await?;
                
                let stmt_res = txguard.conn().prepare(&sql).await;
                if let Err(e) = stmt_res {
                    txguard.rollback().await?;
                    return Err(to_py_err(e));
                }
                let stmt = stmt_res.unwrap();
                let mut total_changes = 0;
                let mut last_id = 0;
                
                if let Some(iterator_obj) = parameters_iter {
                    loop {
                        let chunk_res: PyResult<Vec<libsql_core::params::Params>> = Python::with_gil(|py| {
                            let mut chunk = Vec::with_capacity(100);
                            let iter = iterator_obj.bind(py);
                            for _ in 0..100 {
                                match iter.call_method0("__next__") {
                                    Ok(item) => {
                                        chunk.push(extract_parameters(py, Some(ListOrTuple::extract_bound(&item)?))?);
                                    }
                                    Err(e) => {
                                        if e.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) {
                                            break;
                                        }
                                        return Err(e);
                                    }
                                }
                            }
                            Ok(chunk)
                        });
                        
                        let chunk = match chunk_res {
                            Ok(c) => c,
                            Err(e) => {
                                txguard.rollback().await?;
                                return Err(e);
                            }
                        };
                        
                        if chunk.is_empty() { break; }
                        for p in chunk {
                            if let Err(e) = stmt.execute(p).await {
                                txguard.rollback().await?;
                                return Err(to_py_err(e));
                            }
                            total_changes += txguard.conn().changes() as i64;
                            last_id = txguard.conn().last_insert_rowid();
                        }
                    }
                }
                txguard.commit().await?;
                Ok(PoolCursor {
                    rows: Arc::new(AsyncMutex::new(None)),
                    columns: Arc::new(SyncMutex::new(None)),
                    rowcount: total_changes as i64,
                    lastrowid: last_id as i64,
                    done: Arc::new(AtomicBool::new(true)),
                    arraysize: 1,
                })
            } else {
                Err(PyValueError::new_err("closed"))
            }
        })
    }

    fn executebatch<'py>(
        &self,
        py: Python<'py>,
        operations: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut ops = Vec::new();
        let iter = operations.try_iter()?;
        for item in iter {
            let ob = item?;
            let tuple = ob.downcast::<PyTuple>()?;
            let sql: String = tuple.get_item(0)?.extract()?;
            let params_obj = tuple.get_item(1)?;
            let params = if params_obj.is_none() {
                libsql_core::params::Params::None
            } else {
                extract_parameters(py, Some(ListOrTuple::extract_bound(&params_obj)?))?                
            };
            ops.push((sql, params));
        }
        
        let writer_arc = self.writer.clone();
        let sem = self.writer_sem.clone();
        
        future_into_py(py, async move {
            let _permit = sem.acquire().await.map_err(|e| PyValueError::new_err(e.to_string()))?;
            let conn_opt = { let guard = writer_arc.lock(); guard.as_ref().cloned() };
            if let Some(conn) = conn_opt {
                let txguard = TxGuard::new(conn, true).await?;
                let mut total_changes = 0;
                let mut last_id = 0;
                
                for (sql, params) in ops {
                    let stmt_res = txguard.conn().prepare(&sql).await;
                    if let Err(e) = stmt_res {
                        txguard.rollback().await?;
                        return Err(to_py_err(e));
                    }
                    let stmt = stmt_res.unwrap();
                    if let Err(e) = stmt.execute(params).await {
                        txguard.rollback().await?;
                        return Err(to_py_err(e));
                    }
                    total_changes += txguard.conn().changes() as i64;
                    last_id = txguard.conn().last_insert_rowid();
                }
                txguard.commit().await?;
                Ok(PoolCursor {
                    rows: Arc::new(AsyncMutex::new(None)),
                    columns: Arc::new(SyncMutex::new(None)),
                    rowcount: total_changes as i64,
                    lastrowid: last_id as i64,
                    done: Arc::new(AtomicBool::new(true)),
                    arraysize: 1,
                })
            } else {
                Err(PyValueError::new_err("closed"))
            }
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let writer = self.writer.clone();
        let readers = self.readers.clone();
        future_into_py(py, async move {
            writer.lock().take();
            for r in readers {
                r.lock().take();
            }
            Ok(())
        })
    }

    fn __aenter__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move { Ok(slf) })
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<PyObject>,
        _exc_val: Option<PyObject>,
        _exc_tb: Option<PyObject>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let writer = self.writer.clone();
        let readers = self.readers.clone();
        future_into_py(py, async move {
            writer.lock().take();
            for r in readers {
                r.lock().take();
            }
            Ok(false)
        })
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PoolCursor {
    #[pyo3(get, set)]
    arraysize: usize,
    rows: Arc<AsyncMutex<Option<libsql_core::Rows>>>,
    columns: Arc<SyncMutex<Option<Vec<String>>>>,
    rowcount: i64,
    lastrowid: i64,
    done: Arc<AtomicBool>,
}

#[pymethods]
impl PoolCursor {
    fn fetchone<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rows_arc = self.rows.clone();
        future_into_py(py, async move {
            let mut guard = rows_arc.lock().await;
            if let Some(rows) = guard.as_mut() {
                if let Some(r) = rows.next().await.map_err(to_py_err)? {
                    let cc = rows.column_count();
                    let mut vals = Vec::with_capacity(cc as usize);
                    for i in 0..cc {
                        vals.push(r.get_value(i).map_err(to_py_err)?);
                    }
                    drop(guard);
                    return Python::with_gil(|py| {
                        let mut py_vals = Vec::with_capacity(cc as usize);
                        for v in vals {
                            py_vals.push(convert_value(py, v)?);
                        }
                        Ok(PyTuple::new(py, py_vals)?.unbind().into_any())
                    });
                }
            }
            Python::with_gil(|py| Ok(py.None()))
        })
    }

    fn fetchall<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rows_arc = self.rows.clone();
        future_into_py(py, async move {
            let mut guard = rows_arc.lock().await;
            let mut data = Vec::new();
            if let Some(rows) = guard.as_mut() {
                let cc = rows.column_count();
                while let Some(r) = rows.next().await.map_err(to_py_err)? {
                    let mut row = Vec::with_capacity(cc as usize);
                    for i in 0..cc {
                        row.push(r.get_value(i).map_err(to_py_err)?);
                    }
                    data.push(row);
                }
            }
            drop(guard);
            Python::with_gil(|py| {
                let mut elements = Vec::with_capacity(data.len());
                for row in data {
                    let mut py_row = Vec::with_capacity(row.len());
                    for v in row {
                        py_row.push(convert_value(py, v)?);
                    }
                    elements.push(PyTuple::new(py, py_row)?.unbind().into_any());
                }
                Ok(PyList::new(py, elements)?.unbind().into_any())
            })
        })
    }

    #[pyo3(signature = (size=None))]
    fn fetchmany<'py>(
        &self,
        py: Python<'py>,
        size: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rows_arc = self.rows.clone();
        let done_arc = self.done.clone();
        let arraysize = self.arraysize;
        future_into_py(py, async move {
            let size = size.unwrap_or(arraysize);
            let mut guard = rows_arc.lock().await;
            let mut data = Vec::new();
            if let Some(rows) = guard.as_mut() {
                if !done_arc.load(Ordering::SeqCst) {
                    let cc = rows.column_count();
                    for _ in 0..size {
                        match rows.next().await.map_err(to_py_err)? {
                            Some(r) => {
                                let mut row = Vec::with_capacity(cc as usize);
                                for i in 0..cc {
                                    row.push(r.get_value(i).map_err(to_py_err)?);
                                }
                                data.push(row);
                            }
                            None => {
                                done_arc.store(true, Ordering::SeqCst);
                                break;
                            }
                        }
                    }
                }
            }
            drop(guard);
            Python::with_gil(|py| {
                let mut elements = Vec::with_capacity(data.len());
                for row in data {
                    let mut py_row = Vec::with_capacity(row.len());
                    for v in row {
                        py_row.push(convert_value(py, v)?);
                    }
                    elements.push(PyTuple::new(py, py_row)?.unbind().into_any());
                }
                Ok(PyList::new(py, elements)?.unbind().into_any())
            })
        })
    }

    #[getter]
    fn description(&self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        let guard = self.columns.lock();
        if let Some(cols) = guard.as_ref() {
            let mut elements = Vec::new();
            for name in cols {
                let e = (
                    name.clone(),
                    py.None(),
                    py.None(),
                    py.None(),
                    py.None(),
                    py.None(),
                    py.None(),
                )
                    .into_pyobject(py)?
                    .into_any()
                    .unbind();
                elements.push(e);
            }
            Ok(Some(PyTuple::new(py, elements)?.unbind().into()))
        } else {
            Ok(None)
        }
    }

    #[getter]
    fn lastrowid(&self) -> i64 {
        self.lastrowid
    }

    #[getter]
    fn rowcount(&self) -> i64 {
        self.rowcount
    }

    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let rows_arc = self.rows.clone();
        future_into_py(py, async move {
            let mut guard = rows_arc.lock().await;
            if let Some(rows) = guard.as_mut() {
                if let Some(r) = rows.next().await.map_err(to_py_err)? {
                    let cc = rows.column_count();
                    let mut vals = Vec::with_capacity(cc as usize);
                    for i in 0..cc {
                        vals.push(r.get_value(i).map_err(to_py_err)?);
                    }
                    drop(guard);
                    return Python::with_gil(|py| {
                        let mut py_vals = Vec::with_capacity(cc as usize);
                        for v in vals {
                            py_vals.push(convert_value(py, v)?);
                        }
                        PyTuple::new(py, py_vals).map(|t| t.into_any().unbind())
                    });
                }
            }
            Err(pyo3::exceptions::PyStopAsyncIteration::new_err("done"))
        }).map(Some)
    }
}

#[pyfunction]
#[pyo3(
    signature = (
        database,
        size=10,
        timeout=5.0,
        encryption_key=None
    )
)]
fn create_pool<'py>(
    py: Python<'py>,
    database: String,
    size: usize,
    timeout: f64,
    encryption_key: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    if size < 2 {
        return Err(PyValueError::new_err("pool size must be at least 2"));
    }
    future_into_py(py, async move {
        let encryption_config = match encryption_key {
            Some(key) => {
                let cipher = libsql_core::Cipher::default();
                Some(libsql_core::EncryptionConfig::new(cipher, key.into()))
            }
            None => None,
        };
        let mut builder = libsql_core::Builder::new_local(&database);
        if let Some(config) = encryption_config.clone() {
            builder = builder.encryption_config(config);
        }
        let db = builder.build().await.map_err(to_py_err)?;

        let writer_conn = db.connect().map_err(to_py_err)?;
        writer_conn.busy_timeout(Duration::from_secs_f64(timeout)).map_err(to_py_err)?;
        writer_conn
            .execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000; PRAGMA cache_size=-8000; PRAGMA mmap_size=2147483648; PRAGMA temp_store=MEMORY;",
            )
            .await
            .map_err(to_py_err)?;

        let reader_count = size - 1;
        let mut readers = Vec::with_capacity(reader_count);
        for _ in 0..reader_count {
            let reader = db.connect().map_err(to_py_err)?;
            reader.busy_timeout(Duration::from_secs_f64(timeout)).map_err(to_py_err)?;
            reader
                .execute_batch(
                    "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA query_only=ON; PRAGMA cache_size=-8000; PRAGMA mmap_size=2147483648;",
                )
                .await
                .map_err(to_py_err)?;
            readers.push(Arc::new(SyncMutex::new(Some(reader))));
        }

        Ok(ConnectionPool {
            db: Arc::new(db),
            writer: Arc::new(SyncMutex::new(Some(writer_conn))),
            readers,
            reader_idx: Arc::new(AtomicUsize::new(0)),
            reader_sem: Arc::new(Semaphore::new(reader_count * 2)),
            writer_sem: Arc::new(Semaphore::new(1)),
            pool_size: size,
        })
    })
}

#[pymodule]
fn aiolibsql(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("VERSION", VERSION)?;
    m.add("LEGACY_TRANSACTION_CONTROL", LEGACY_TRANSACTION_CONTROL)?;
    m.add("paramstyle", "qmark")?;
    m.add("sqlite_version_info", (3, 42, 0))?;
    
    m.add("Error", py.get_type::<DatabaseError>())?;
    m.add("DatabaseError", py.get_type::<DatabaseError>())?;
    m.add("OperationalError", py.get_type::<OperationalError>())?;
    m.add("IntegrityError", py.get_type::<IntegrityError>())?;
    m.add("TimeoutError", py.get_type::<TimeoutError>())?;

    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(create_pool, m)?)?;
    m.add_class::<Connection>()?;
    m.add_class::<Cursor>()?;
    m.add_class::<ConnectionPool>()?;
    m.add_class::<PoolCursor>()?;
    Ok(())
}
