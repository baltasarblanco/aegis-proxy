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

const BIND_ADDR: &str = "0.0.0.0:8081";      // <--- El Escudo Frontal (AEGIS)
const CHRONOS_ADDR: &str = "127.0.0.1:8080"; // <--- La Bóveda LSM (Chronos)
const TCP_BACKLOG: i32 = 4096;
const BUFFER_SIZE: usize = 4096;
const POOL_CAPACITY: usize = 1024;
const MAX_HOT_PIPES: usize = 64;             // <--- FASE 5: Máximo de conexiones persistentes por núcleo

struct Telemetry {
    active_connections: AtomicUsize,
    total_bytes: AtomicUsize,
}

struct ConnectionGuard {
    tele: Arc<Telemetry>,
}
impl ConnectionGuard {
    fn new(tele: Arc<Telemetry>) -> Self {
        tele.active_connections.fetch_add(1, Ordering::Relaxed);
        Self { tele }
    }
}
impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.tele.active_connections.fetch_sub(1, Ordering::Relaxed);
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
        active_connections: AtomicUsize::new(0),
        total_bytes: AtomicUsize::new(0),
    });

    for core_id in core_ids {
        let mut shutdown_rx = shutdown_tx.subscribe();
        let tele_clone = telemetry.clone();

        let handle = thread::spawn(move || {
            core_affinity::set_for_current(core_id);
            
            // Pool de Memoria (RAM)
            let mut local_pool = VecDeque::with_capacity(POOL_CAPACITY);
            for _ in 0..POOL_CAPACITY {
                local_pool.push_back(Vec::with_capacity(BUFFER_SIZE));
            }
            let pool = Rc::new(RefCell::new(local_pool));

            // 💥 FASE 5: Pool de Conexiones Persistentes (Tuberías Calientes)
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

                                    // 1. OBTENER TUBERÍA CALIENTE: Extraemos una o conectamos una nueva si no hay
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

                                    // 2. LECTURA DEL CLIENTE
                                    let (read_res, buf_read) = stream.read(buf).await;

                                    if let Ok(n) = read_res {
                                        if n > 0 {
                                            task_telemetry.total_bytes.fetch_add(n, Ordering::Relaxed);

                                            // 3. DISPARO A CHRONOS
                                            let (write_res, mut buf_written) = chronos_stream.write_all(buf_read).await;
                                            
                                            if write_res.is_ok() {
                                                buf_written.clear(); 
                                                
                                                // 4. LECTURA DE LA BÓVEDA
                                                let (resp_res, buf_resp) = chronos_stream.read(buf_written).await;

                                                if let Ok(resp_n) = resp_res {
                                                    if resp_n > 0 {
                                                        task_telemetry.total_bytes.fetch_add(resp_n, Ordering::Relaxed);

                                                        // 5. RESPUESTA FINAL AL CLIENTE
                                                        let (_final_res, mut buf_final) = stream.write_all(buf_resp).await;
                                                        buf_final.clear();
                                                        
                                                        // 💥 FASE 5: Misión cumplida. Reciclamos la memoria Y la conexión.
                                                        pool_ref.borrow_mut().push_back(buf_final);
                                                        if conn_pool_ref.borrow().len() < MAX_HOT_PIPES {
                                                            conn_pool_ref.borrow_mut().push_back(chronos_stream);
                                                        }
                                                        return;
                                                    }
                                                }
                                                // Si Chronos falla, no reciclamos la conexión (se destruirá sola)
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

    println!("🛡️ [AEGIS CONTROL PLANE] Todos los sistemas nominales. HUD Activado.");
    let mut ticker = interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\n⚠️ [AEGIS CONTROL PLANE] Ctrl+C detectado. Iniciando apagado de la Hidra...");
                break;
            }
            _ = ticker.tick() => {
                let conns = telemetry.active_connections.load(Ordering::Relaxed);
                let bytes = telemetry.total_bytes.load(Ordering::Relaxed);
                let mb = bytes as f64 / 1_048_576.0;
                
                if conns > 0 || bytes > 0 {
                    println!("📊 [HUD] Conexiones Activas: {} | Tráfico Total: {:.4} MB", conns, mb);
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