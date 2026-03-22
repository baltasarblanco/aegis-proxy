use socket2::{Domain, Protocol, Socket, Type};
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::thread;
use tokio::sync::broadcast;
use tokio_uring::net::TcpListener;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::time::{interval, Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt}; // <--- Necesario para el micro-servidor HTTP

const BIND_ADDR: &str = "0.0.0.0:8081";      // <--- El Escudo Frontal (AEGIS)
const CHRONOS_ADDR: &str = "127.0.0.1:8080"; // <--- La Bóveda LSM (Chronos)
const TCP_BACKLOG: i32 = 4096;
const BUFFER_SIZE: usize = 4096;
const POOL_CAPACITY: usize = 1024;
const MAX_HOT_PIPES: usize = 64;

// 💥 RICHARDS VECTOR: Acolchado de Caché (Cache Padding) a 64 bytes.
#[repr(align(64))]
struct CachePadded<T>(T);

struct Telemetry {
    active_connections: CachePadded<AtomicUsize>,
    total_bytes: CachePadded<AtomicUsize>,
}

struct ConnectionGuard {
    tele: Arc<Telemetry>,
}
impl ConnectionGuard {
    fn new(tele: Arc<Telemetry>) -> Self {
        tele.active_connections.0.fetch_add(1, Ordering::Relaxed);
        Self { tele }
    }
}
impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.tele.active_connections.0.fetch_sub(1, Ordering::Relaxed);
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    println!("⚙️ [AEGIS CONTROL PLANE] Iniciando secuencia de arranque...");

    let core_ids = core_affinity::get_core_ids().expect("Error crítico: No se puede leer la topología");
    let (shutdown_tx, _) = broadcast::channel::<()>(16);
    let addr: SocketAddr = BIND_ADDR.parse().expect("Dirección IP/Puerto inválidos");
    let mut handles = vec![];

    let telemetry = Arc::new(Telemetry {
        active_connections: CachePadded(AtomicUsize::new(0)),
        total_bytes: CachePadded(AtomicUsize::new(0)),
    });

    // --- INICIO DEL PLANO DE DATOS (io_uring workers) ---
    for core_id in core_ids {
        let mut shutdown_rx = shutdown_tx.subscribe();
        let tele_clone = telemetry.clone();

        let handle = thread::spawn(move || {
            core_affinity::set_for_current(core_id);
            
            let mut local_pool = VecDeque::with_capacity(POOL_CAPACITY);
            for _ in 0..POOL_CAPACITY {
                local_pool.push_back(Vec::with_capacity(BUFFER_SIZE));
            }
            let pool = Rc::new(RefCell::new(local_pool));

            let local_conn_pool = VecDeque::with_capacity(MAX_HOT_PIPES);
            let conn_pool = Rc::new(RefCell::new(local_conn_pool));

            let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)).unwrap();
            socket.set_reuse_port(true).unwrap();
            socket.set_reuse_address(true).unwrap();
            socket.set_nonblocking(true).unwrap();
            socket.bind(&addr.into()).unwrap();
            socket.listen(TCP_BACKLOG).unwrap();
            let std_listener: StdTcpListener = socket.into();
            
            tokio_uring::start(async move {
                let listener = TcpListener::from_std(std_listener);
                println!("🚀 [AEGIS-CORE-{}] Motor y Tuberías encendidas.", core_id.id);

                loop {
                    tokio::select! {
                        accept_res = listener.accept() => {
                            if let Ok((stream, _peer_addr)) = accept_res {
                                let pool_ref = pool.clone();
                                let conn_pool_ref = conn_pool.clone();
                                let task_telemetry = tele_clone.clone();
                                
                                tokio_uring::spawn(async move {
                                    let _guard = ConnectionGuard::new(task_telemetry.clone());

                                    let mut buf = pool_ref.borrow_mut().pop_front()
                                        .unwrap_or_else(|| Vec::with_capacity(BUFFER_SIZE));
                                    buf.clear(); 

                                    let chronos_stream = if let Some(hot_pipe) = conn_pool_ref.borrow_mut().pop_front() {
                                        hot_pipe
                                    } else {
                                        let backend_addr: SocketAddr = CHRONOS_ADDR.parse().unwrap();
                                        match tokio_uring::net::TcpStream::connect(backend_addr).await {
                                            Ok(s) => s,
                                            Err(_) => {
                                                pool_ref.borrow_mut().push_back(buf);
                                                return;
                                            }
                                        }
                                    };

                                    let (read_res, buf_read) = stream.read(buf).await;

                                    if let Ok(n) = read_res {
                                        if n > 0 {
                                            task_telemetry.total_bytes.0.fetch_add(n, Ordering::Relaxed);

                                            let (write_res, mut buf_written) = chronos_stream.write_all(buf_read).await;
                                            
                                            if write_res.is_ok() {
                                                buf_written.clear(); 
                                                
                                                let (resp_res, buf_resp) = chronos_stream.read(buf_written).await;

                                                if let Ok(resp_n) = resp_res {
                                                    if resp_n > 0 {
                                                        task_telemetry.total_bytes.0.fetch_add(resp_n, Ordering::Relaxed);

                                                        let (_final_res, mut buf_final) = stream.write_all(buf_resp).await;
                                                        buf_final.clear();
                                                        
                                                        pool_ref.borrow_mut().push_back(buf_final);
                                                        if conn_pool_ref.borrow().len() < MAX_HOT_PIPES {
                                                            conn_pool_ref.borrow_mut().push_back(chronos_stream);
                                                        }
                                                        return;
                                                    }
                                                }
                                                let mut safe_buf = buf_resp; safe_buf.clear();
                                                pool_ref.borrow_mut().push_back(safe_buf);
                                                return;
                                            }
                                            let mut safe_buf = buf_written; safe_buf.clear();
                                            pool_ref.borrow_mut().push_back(safe_buf);
                                            return;
                                        }
                                    }
                                    let mut safe_buf = buf_read; safe_buf.clear();
                                    pool_ref.borrow_mut().push_back(safe_buf);
                                });
                            }
                        }
                        _ = shutdown_rx.recv() => break,
                    }
                }
            });
        });
        handles.push(handle);
    }
    // --- FIN DEL PLANO DE DATOS ---

    // --- INICIO DEL PLANO DE CONTROL ---
    println!("🛡️ [AEGIS CONTROL PLANE] Todos los sistemas nominales. HUD en terminal Activado.");

    // 📡 EL SATÉLITE STARK (Micro-Servidor HTTP para Prometheus)
    let tele_metrics = telemetry.clone();
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind("0.0.0.0:8082").await.expect("Fallo al abrir puerto 8082");
        println!("📡 [PROMETHEUS SATELLITE] Métricas expuestas en http://127.0.0.1:8082/metrics");
        
        loop {
            if let Ok((mut stream, _)) = listener.accept().await {
                let tele = tele_metrics.clone();
                tokio::spawn(async move {
                    let mut buf = [0; 512];
                    let _ = stream.read(&mut buf).await; // Leemos y descartamos la petición GET
                    
                    let conns = tele.active_connections.0.load(Ordering::Relaxed);
                    let bytes = tele.total_bytes.0.load(Ordering::Relaxed);
                    
                    // Formato exacto que requiere Grafana/Prometheus
                    let response = format!(
                        "HTTP/1.1 200 OK\r\n\
                        Content-Type: text/plain; version=0.0.4\r\n\
                        Connection: close\r\n\r\n\
                        # HELP aegis_active_connections Numero de conexiones L4 activas\n\
                        # TYPE aegis_active_connections gauge\n\
                        aegis_active_connections {}\n\
                        # HELP aegis_total_bytes Total de bytes enrutados en la red\n\
                        # TYPE aegis_total_bytes counter\n\
                        aegis_total_bytes {}\n",
                        conns, bytes
                    );
                    
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        }
    });

    let mut ticker = interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\n⚠️ [AEGIS CONTROL PLANE] Ctrl+C detectado. Iniciando apagado de la Hidra...");
                break;
            }
            _ = ticker.tick() => {
                let conns = telemetry.active_connections.0.load(Ordering::Relaxed);
                let bytes = telemetry.total_bytes.0.load(Ordering::Relaxed);
                let mb = bytes as f64 / 1_048_576.0;
                
                if conns > 0 || bytes > 0 {
                    println!("📊 [HUD] Conexiones: {} | Tráfico: {:.4} MB", conns, mb);
                }
            }
        }
    }

    let _ = shutdown_tx.send(());
    for handle in handles {
        handle.join().unwrap();
    }
    println!("💀 [AEGIS CONTROL PLANE] Apagado completo. Exit Code 0.");
}