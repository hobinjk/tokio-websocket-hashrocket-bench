extern crate futures;
extern crate tokio_core;
extern crate tokio_io;
extern crate websocket;
extern crate serde_json;
extern crate net2;

use serde_json::Value;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::io::{Error, ErrorKind, Result};
use std::net::{self, SocketAddr};
use std::thread;

use tokio_core::net::TcpStream;
use tokio_core::reactor::{Core, Handle};
use tokio_io::AsyncRead;

use net2::unix::UnixTcpBuilderExt;

use futures::{Future, Stream, Sink};
use futures::sync::mpsc;

use websocket::{Request, WebSocketCodec, new_text_frame, Opcode, Frame};

const NULL_PAYLOAD: &'static Value = &Value::Null;

enum Message {
    Echo(Frame),
    Broadcast(Frame, Frame),
    None(),
}

fn process_frame(frame: Frame) -> Message {
    if frame.header.opcode == Opcode::Close {
        return Message::Echo(frame);
    }
    if frame.header.opcode != Opcode::Text {
        return Message::None();
    }
    // TODO send back pongs

    let payload = frame.payload_string().unwrap();
    if let Ok(Value::Object(obj)) = serde_json::from_str::<Value>(&payload) {
        if let Some(&Value::String(ref s)) = obj.get("type") {
            if s == "echo" {
                return Message::Echo(frame);
            }
            if s == "broadcast" {
                let msg = format!(r#"{{"type":"broadcastResult","payload":{}}}"#, obj.get("payload").unwrap_or(NULL_PAYLOAD));
                return Message::Broadcast(frame, new_text_frame(&msg, None));
            }
        }
    }
    Message::None()
}

fn listener(addr: &SocketAddr) -> Result<net::TcpListener> {
    let listener = match *addr {
        SocketAddr::V4(_) => try!(net2::TcpBuilder::new_v4()),
        SocketAddr::V6(_) => try!(net2::TcpBuilder::new_v6()),
    };
    try!(listener.reuse_port(true));
    try!(listener.reuse_address(true));
    try!(listener.bind(addr));
    listener.listen(32)
}

type Connections = Arc<RwLock<HashMap<SocketAddr, mpsc::UnboundedSender<Frame>>>>;

fn serve_one(handle: &Handle, connections: &Connections, conn: net::TcpStream, addr: SocketAddr) -> Result<()> {
    let conn = TcpStream::from_stream(conn, handle).unwrap();
    let (sink, stream) = conn.framed(WebSocketCodec::new()).split();
    let (tx, rx) = mpsc::unbounded();

    connections.write().unwrap().insert(addr, tx.clone());

    let connections_inner = connections.clone();
    let reader = stream.for_each(move |req| {
        match req {
            Request::Frame(frame) => {
                match process_frame(frame) {
                    Message::None() => {},
                    Message::Echo(frame) => {
                        if frame.header.opcode == Opcode::Close {
                            connections_inner.write().unwrap().remove(&addr);
                            return Err(Error::new(ErrorKind::Other, "close requested"))
                        }
                        let masked_frame = new_text_frame(&frame.payload_string().unwrap(), None);
                        mpsc::UnboundedSender::send(&tx, masked_frame).unwrap();
                    },
                    Message::Broadcast(broadcast_frame, echo_frame) => {
                        let masked_frame = new_text_frame(&broadcast_frame.payload_string().unwrap(), None);
                        let conns = connections_inner.read().unwrap();
                        for (&t_addr, tx) in conns.iter() {
                            mpsc::UnboundedSender::send(&tx, masked_frame.clone()).unwrap();
                            if addr == t_addr {
                                mpsc::UnboundedSender::send(&tx, echo_frame.clone()).unwrap();
                            }
                        }
                    },
                }
            },
            Request::Open() => {
                mpsc::UnboundedSender::send(&tx, new_text_frame("this message is dropped", None)).unwrap();
            }
        }
        Ok(())
    });
    let connections = connections.clone();
    let writer = rx.map_err(|_| Error::new(ErrorKind::Other, "receiver error")).fold(sink, |sink, msg| {
        sink.send(msg)
    });
    let reader = reader.map_err(|_| Error::new(ErrorKind::Other, "transmitter error"));
    let conn = reader.map(|_| ()).select(writer.map(|_| ()));
    handle.spawn(conn.then(move |_| {
        connections.write().unwrap().remove(&addr);
        Ok(())
    }));
    Ok(())
}

fn serve(incoming: mpsc::UnboundedReceiver<(net::TcpStream, SocketAddr)>, connections: &Connections) {
    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let srv = incoming.map_err(|_| Error::new(ErrorKind::Other, "incoming socket error")).for_each(move |(conn, addr)| {
        serve_one(&handle, &connections, conn, addr)
    });

    core.run(srv).unwrap();
}

fn main() {
    // Set up using skeleton of chat example, use encode and decode directly
    let addr = "0.0.0.0:8084".parse().unwrap();

    let socket = listener(&addr).unwrap();

    let connections = Arc::new(RwLock::new(HashMap::new()));

    let mut incomings = Vec::new();

    let threads = (0..4).map(|_| {
        let (tx, rx) = mpsc::unbounded();
        incomings.push(tx.clone());

        let connections_inner = connections.clone();
        thread::spawn(move || {
            serve(rx, &connections_inner);
        })
    }).collect::<Vec<_>>();

    let mut i = 0;
    for conn in socket.incoming() {
        match conn {
            Ok(conn) => {
                let addr = conn.peer_addr().unwrap();
                mpsc::UnboundedSender::send(&incomings[i], (conn, addr)).unwrap();
                i += 1;
                if i >= incomings.len() {
                    i = 0;
                }
            },
            Err(_) => {
            }
        }
    }

    for thread in threads {
        thread.join().unwrap();
    }
}
