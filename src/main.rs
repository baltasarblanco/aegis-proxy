use core_affinity::CoreId;
use socket2::{Domain, Protocol, Socket, Type};
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::thread;
use tokio::sync::broadcast;
use tokio_uring::net::TcpListener;

const BIND_ADDR: &str = "0.0.0.0:8080";
const TCP_BACKLOG: i32 = 4096;

// EL Plano de Control: Usamos un runtime ligero de un solo hilo para gestionar señales.
// NO usará la CPU a menos que presiones Ctrl+C.
#[tokio::main(flavor = "current_thread")]
async fn main() {
    println!("⚙️ [AEGIS CONTROL PLANE] Iniciando secuencia de arranque...");

    // 1. Detección de topología
    let core_ids = core_affinity::get_core_ids().expect("Error crítico: No se puede leer la topología");
    
    // 2. EL Botón del Pánico (Canal de Radio Broadcast)
    // EL canal tiene capacidad para 16 mensajes en vuelo.
    let (shutdown_tx, _) = broadcast::channel::<()>(16);

    let addr: SocketAddr = BIND_ADDR.parse().expect("Dirección IP/Puerto inválidos");
    let mut handles = vec![];

    // 3. Despliege del Plano de Datos (Workers)
    for core_id in core_ids {
        // Entregamos un receptor de radio a cada clon antes de que nazca
        let mut shutdown_rx = shutdown_tx.subscribe();

        let handle = thread::spawn(move || {
            // Afinidad y Forja (Idéntico a la versión anterior)
            core_affinity::set_for_current(core_id);
            let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)).unwrap();
            socket.set_reuse_port(true).unwrap();
            socket.set_reuse_address(true).unwrap();
            socket.set_nonblocking(true).unwrap();
            socket.bind(&addr.into()).unwrap();
            socket.listen(TCP_BACKLOG).unwrap();

            let std_listener: StdTcpListener = socket.into();

            // Ignición del runtime io_uring para este núcleo
            tokio_uring::start(async move {
                let listener = TcpListener::from_std(std_listener);
                println!("🚀 [AEGIS-CORE-{}] En línea y a la escucha.", core_id.id);

                // EL Bucle Infinito del Worker
                loop {
                    // tokio::select! nos permite escuchar dos futuros al mismo tiempo.
                    tokio::select! {
                        // Evento A: Llega un cliente por hardware
                        accept_res = listener.accept() => {
                            match accept_res {
                                Ok((_stream, _peer_addr)) => {
                                    // Cliente recibido en el anillo. Lo soltamos (Cierre TCP inmediato).
                                } 
                                Err(e) => { 
                                    eprintln!("Error en núcleo {}: {}", core_id.id, e); 
                                }
                            }
                        }
                        // Evento B: EL Plano de Control grita por la radio
                        _ = shutdown_rx.recv() => {
                            println!("🛑 [AEGIS-CORE-{}] Señal de apagado recibida. Purgando anillo y terminando.", core_id.id);
                            break; // Rompemos el bucle infinito. El runtime io_uring se apagará limpiamente.
                        }
                    }
                }
            });
        });

        handles.push(handle);
    }

    // 4. El Letargo del Director
    println!("🛡️ [AEGIS CONTROL PLANE] Todos los sistemas nominales. Presiona Ctrl+C para apagado quirúrgico.");
    
    // El hilo principal se duerme aquí, esperando la señal SIGINT del Sistema Operativo.
    tokio::signal::ctrl_c().await.expect("Falla al instalar el manejador de Ctrl+C");

    // 5. Secuencia de Apagado Quirúrgico
    println!("\n⚠️ [AEGIS CONTROL PLANE] Ctrl+C detectado. Iniciando apagado de la Hidra...");
    
    // Enviamos un único mensaje por la radio. Los 16 receptores lo escucharán simultáneamente.
    let _ = shutdown_tx.send(());

    // Esperamos a que cada hilo del SO termine su bucle, cierre sus sockets y devuelva la RAM.
    for handle in handles {
        handle.join().unwrap();
    }

    println!("💀 [AEGIS CONTROL PLANE] Apagado completo. Exit Code 0.");
}



