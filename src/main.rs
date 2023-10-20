mod request;
mod response;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use parking_lot::RwLock;
use rand::{Rng, SeedableRng};
use request::write_to_stream;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::delay_for;

#[derive(Debug, Clone)]
enum Upstream {
    Alive(String),
    Dead(String),
}

/// Contains information parsed from the command-line invocation of balancebeam. The Clap macros
/// provide a fancy way to automatically construct a command-line argument parser.
#[derive(Parser, Debug)]
#[command(about = "Fun with load balancing")]
struct CmdOptions {
    #[arg(
        short,
        long,
        help = "IP/port to bind to",
        default_value = "0.0.0.0:1100"
    )]
    bind: String,
    #[arg(short, long, help = "Upstream host to forward requests to")]
    upstream: Vec<String>,
    #[arg(
        long,
        help = "Perform active health checks on this interval (in seconds)",
        default_value = "10"
    )]
    active_health_check_interval: usize,
    #[arg(
        long,
        help = "Path to send request to for active health checks",
        default_value = "/"
    )]
    active_health_check_path: String,
    #[arg(
        long,
        help = "Maximum number of requests to accept per IP per minute (0 = unlimited)",
        default_value = "0"
    )]
    max_requests_per_minute: usize,
}

/// Contains information about the state of balancebeam (e.g. what servers we are currently proxying
/// to, what servers have failed, rate limiting counts, etc.)
///
/// You should add fields to this struct in later milestones.
struct ProxyState {
    /// How frequently we check whether upstream servers are alive (Milestone 4)
    active_health_check_interval: usize,
    /// Where we should send requests when doing active health checks (Milestone 4)
    active_health_check_path: String,
    /// Maximum number of requests an individual IP can make in a minute (Milestone 5)
    #[allow(dead_code)]
    max_requests_per_minute: usize,
    // Hash map to map between the upstream and the number of request the server is handling
    rate_tracker: HashMap<String, usize>,
    /// Addresses of servers that we are proxying to
    upstream_addresses: Vec<String>,
    /// Upsreama addresses that have died
    dead_upstream: Vec<String>,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    // Initialize the logging library. You can print log messages using the `log` macros:
    // https://docs.rs/log/0.4.8/log/ You are welcome to continue using print! statements; this
    // just looks a little prettier.
    if let Err(_) = std::env::var("RUST_LOG") {
        std::env::set_var("RUST_LOG", "debug");
    }
    pretty_env_logger::init();

    // Parse the command line arguments passed to this program
    let options = CmdOptions::parse();
    if options.upstream.len() < 1 {
        log::error!("At least one upstream server must be specified using the --upstream option.");
        std::process::exit(1);
    }

    // Start listening for connections
    let mut listener = match TcpListener::bind(&options.bind).await {
        Ok(listener) => listener,
        Err(err) => {
            log::error!("Could not bind to {}: {}", options.bind, err);
            std::process::exit(1);
        }
    };
    log::info!("Listening for requests on {}", options.bind);
    let upstream_capacity = options.upstream.len();
    // Handle incoming connections
    let state = ProxyState {
        rate_tracker: HashMap::new(),
        upstream_addresses: options.upstream,
        active_health_check_interval: options.active_health_check_interval,
        active_health_check_path: options.active_health_check_path,
        max_requests_per_minute: options.max_requests_per_minute,
        dead_upstream: Vec::with_capacity(upstream_capacity),
    };
    let interval = Duration::from_secs(state.active_health_check_interval as u64);

    let state = Arc::new(RwLock::new(state));

    let periodic_state = state.clone();

    // task to perform health check periodically
    tokio::spawn(async move {
        periodic_health_check(periodic_state, interval).await;
    });

    // task to reset counter at the end of the windows for rate limiting
    let counter_state = state.clone();
    tokio::spawn(async move {
        loop {
            delay_for(Duration::from_secs(60)).await;
            let mut state = counter_state.write();
            for (_key, value) in state.rate_tracker.iter_mut() {
                *value = 0;
            }
        }
    });

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(value) => value,
            Err(_err) => {
                break;
            }
        };

        let state = state.clone();
        tokio::spawn(async move {
            handle_connection(stream, state).await;
        });
    }
    Ok(())
}

async fn health_check(upstream: String, path: String) -> Upstream {
    let request = http::Request::builder()
        .method(http::Method::GET)
        .uri(&path)
        .header("Host", &upstream)
        .body(Vec::new())
        .unwrap();
    let mut stream = match TcpStream::connect(&upstream).await {
        Err(err) => {
            log::error!("Server {} is  dead when connecting: {}", upstream, err);
            return Upstream::Dead(upstream.clone());
        }
        Ok(stream) => stream,
    };

    if let Err(err) = write_to_stream(&request, &mut stream).await {
        log::error!(
            "Server {} is  dead when writing to stream: {}",
            upstream,
            err
        );
        return Upstream::Dead(upstream.clone());
    }

    let status = match response::read_from_stream(&mut stream, request.method()).await {
        Err(err) => {
            log::error!(
                "Server {} is  dead when receiving response: {:?}",
                upstream,
                err
            );
            return Upstream::Dead(upstream.clone());
        }
        Ok(resp) => resp.status(),
    };

    if status != 200 {
        log::error!("Server {} is still dead with status {}", upstream, status);
        return Upstream::Dead(upstream.clone());
    }

    return Upstream::Alive(upstream.clone());
}

async fn periodic_health_check(state: Arc<RwLock<ProxyState>>, interval: Duration) {
    loop {
        delay_for(interval).await;
        let mut upstream_markers = Vec::new();
        let mut handles: Vec<JoinHandle<Upstream>> = Vec::new();
        let (upstreams, path) = {
            let state = state.read();
            (
                state
                    .upstream_addresses
                    .clone()
                    .into_iter()
                    .chain(state.dead_upstream.clone().into_iter())
                    .collect::<Vec<String>>(),
                state.active_health_check_path.clone(),
            )
        };
        for upstream in upstreams {
            let handle = tokio::spawn(health_check(upstream, path.clone()));
            handles.push(handle);
        }
        for handle in handles {
            let upstream = match handle.await {
                Ok(upstream) => upstream,
                Err(err) => {
                    log::error!("Error in joining durring health check: {}", err);
                    continue;
                }
            };
            upstream_markers.push(upstream);
        }
        if upstream_markers.iter().all(|e| match e {
            Upstream::Alive(_) => true,
            Upstream::Dead(_) => false,
        }) {
            continue;
        }
        let mut state = state.write();
        state.dead_upstream.clear();
        state.upstream_addresses.clear();
        for ele in upstream_markers {
            match ele {
                Upstream::Alive(upstream) => state.upstream_addresses.push(upstream),
                Upstream::Dead(upstream) => state.dead_upstream.push(upstream),
            }
        }
    }
}
async fn connect_to_upstream(state: Arc<RwLock<ProxyState>>) -> Result<TcpStream, std::io::Error> {
    let mut rng = rand::rngs::StdRng::from_entropy();
    loop {
        let upstream_ip = {
            let state = state.read();
            let upstream_idx = rng.gen_range(0, state.upstream_addresses.len());
            state.upstream_addresses[upstream_idx].clone()
        };
        match TcpStream::connect(&upstream_ip).await {
            Err(err) => {
                log::error!("Failed to connect to upstream {}: {}", upstream_ip, err);
                {
                    let mut state = state.write();
                    if state.upstream_addresses.is_empty() {
                        log::error!("All upstreams have died");
                        return Err(err);
                    }
                    let dead_upstream_idx = match state
                        .upstream_addresses
                        .iter()
                        .position(|addr| *addr == upstream_ip)
                    {
                        Some(idx) => idx,
                        None => return Err(err),
                    };
                    let dead_upstream = state.upstream_addresses.swap_remove(dead_upstream_idx);
                    log::error!("Upstream {} has died", dead_upstream);
                    state.dead_upstream.push(dead_upstream);
                }
                continue;
            }
            Ok(stream) => return Ok(stream),
        };
    }
}

async fn send_response(client_conn: &mut TcpStream, response: &http::Response<Vec<u8>>) {
    let client_ip = client_conn.peer_addr().unwrap().ip().to_string();
    log::info!(
        "{} <- {}",
        client_ip,
        response::format_response_line(&response)
    );
    if let Err(error) = response::write_to_stream(&response, client_conn).await {
        log::warn!("Failed to send response to client: {}", error);
        return;
    }
}

fn is_over_rate_limit(client_ip: &String, state: Arc<RwLock<ProxyState>>) -> bool {
    let mut rate_state = state.write();
    if rate_state.max_requests_per_minute == 0 {
        false
    } else {
        let count = rate_state
            .rate_tracker
            .entry(client_ip.clone())
            .and_modify(|e| *e += 1)
            .or_insert(1);
        if *count > rate_state.max_requests_per_minute {
            true
        } else {
            false
        }
    }
}

async fn handle_connection(mut client_conn: TcpStream, state: Arc<RwLock<ProxyState>>) {
    let client_ip = client_conn.peer_addr().unwrap().ip().to_string();
    log::info!("Connection received from {}", client_ip);

    // Rate limiting per client ip
    if is_over_rate_limit(&client_ip, state.clone()) {
        log::info!("Ip {} sends too many requests", client_ip);
        let response = response::make_http_error(http::StatusCode::TOO_MANY_REQUESTS);
        send_response(&mut client_conn, &response).await;
        return;
    }

    // Open a connection to a random destination server
    let mut upstream_conn = match connect_to_upstream(state).await {
        Ok(stream) => stream,
        Err(_error) => {
            let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
            send_response(&mut client_conn, &response).await;
            return;
        }
    };
    let upstream_ip = client_conn.peer_addr().unwrap().ip().to_string();

    // The client may now send us one or more requests. Keep trying to read requests until the
    // client hangs up or we get an error.
    loop {
        // Read a request from the client
        let mut request = match request::read_from_stream(&mut client_conn).await {
            Ok(request) => request,
            // Handle case where client closed connection and is no longer sending requests
            Err(request::Error::IncompleteRequest(0)) => {
                log::debug!("Client finished sending requests. Shutting down connection");
                return;
            }
            // Handle I/O error in reading from the client
            Err(request::Error::ConnectionError(io_err)) => {
                log::info!("Error reading request from client stream: {}", io_err);
                return;
            }
            Err(error) => {
                log::debug!("Error parsing request: {:?}", error);
                let response = response::make_http_error(match error {
                    request::Error::IncompleteRequest(_)
                    | request::Error::MalformedRequest(_)
                    | request::Error::InvalidContentLength
                    | request::Error::ContentLengthMismatch => http::StatusCode::BAD_REQUEST,
                    request::Error::RequestBodyTooLarge => http::StatusCode::PAYLOAD_TOO_LARGE,
                    request::Error::ConnectionError(_) => http::StatusCode::SERVICE_UNAVAILABLE,
                });
                send_response(&mut client_conn, &response).await;
                continue;
            }
        };
        log::info!(
            "{} -> {}: {}",
            client_ip,
            upstream_ip,
            request::format_request_line(&request)
        );

        // Add X-Forwarded-For header so that the upstream server knows the client's IP address.
        // (We're the ones connecting directly to the upstream server, so without this header, the
        // upstream server will only know our IP, not the client's.)
        request::extend_header_value(&mut request, "x-forwarded-for", &client_ip);

        // Forward the request to the server
        if let Err(error) = request::write_to_stream(&request, &mut upstream_conn).await {
            log::error!(
                "Failed to send request to upstream {}: {}",
                upstream_ip,
                error
            );
            let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
            send_response(&mut client_conn, &response).await;
            return;
        }
        log::debug!("Forwarded request to server");

        // Read the server's response
        let response = match response::read_from_stream(&mut upstream_conn, request.method()).await
        {
            Ok(response) => response,
            Err(error) => {
                log::error!("Error reading response from server: {:?}", error);
                let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
                send_response(&mut client_conn, &response).await;
                return;
            }
        };
        // Forward the response to the client
        send_response(&mut client_conn, &response).await;
        log::debug!("Forwarded response to client");
    }
}
