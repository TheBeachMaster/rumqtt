use std::cell::RefCell;
use std::rc::Rc;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::time::Duration;
use std::thread;
use std::io::{self, ErrorKind};

use futures::{future, Future, Sink};
use futures::stream::{Stream, SplitStream};
use futures::sync::mpsc::Receiver;
use futures::unsync;
use futures::unsync::mpsc::UnboundedSender;
use tokio_core::reactor::{Core, Interval};
use tokio_core::net::TcpStream;
use tokio_io::AsyncRead;
use tokio_io::codec::Framed;

use native_tls::TlsConnector;
use native_tls::Certificate;
use tokio_tls::{TlsConnectorExt, TlsStream};

use mqtt3::Packet;

use error::ConnectError;
use mqttopts::{MqttOptions, ReconnectOptions, SecurityOptions};
use client::state::MqttState;
use codec::MqttCodec;
use crossbeam_channel;

pub struct Connection {
    notifier_tx: crossbeam_channel::Sender<Packet>,
    mqtt_state: Rc<RefCell<MqttState>>,
    opts: MqttOptions,
    reactor: Core,
}

impl Connection {
    pub fn new(opts: MqttOptions, notifier_tx: crossbeam_channel::Sender<Packet>) -> Self {
        Connection {
            notifier_tx: notifier_tx,
            mqtt_state: Rc::new(RefCell::new(MqttState::new(opts.clone()))),
            opts: opts,
            reactor: Core::new().unwrap()
        }
    }

    pub fn start(&mut self, mut commands_rx: Receiver<Packet>) {
        let initial_connect = self.mqtt_state.borrow().initial_connect();
        let reconnect_opts = self.opts.reconnect;

        'reconnect: loop {
            let framed = match self.mqtt_connect() {
                Ok(framed) => framed,
                Err(e) => {
                    println!("Connection error = {:?}", e);
                    match reconnect_opts {
                        ReconnectOptions::Never => break 'reconnect,
                        ReconnectOptions::AfterFirstSuccess(d) if !initial_connect => {
                            info!("Will retry connecting again in {} seconds", d);
                            thread::sleep(Duration::new(u64::from(d), 0));
                            continue 'reconnect;
                        }
                        ReconnectOptions::AfterFirstSuccess(_) => break 'reconnect,
                        ReconnectOptions::Always(d) => {
                            info!("Will retry connecting again in {} seconds", d);
                            thread::sleep(Duration::new(u64::from(d), 0));
                            continue 'reconnect;
                        }
                    }
                }
            };

            let (network_reply_tx, mut network_reply_rx) = unsync::mpsc::unbounded::<Packet>();

            let (mut sender, receiver) = framed.split();
            let mqtt_recv = self.mqtt_network_recv_future(receiver, network_reply_tx.clone());
            let ping_timer = self.ping_timer_future(network_reply_tx.clone());

            // republish last session unacked packets
            // NOTE: this will block eventloop until last session publishs are written to network
            // TODO: verify for duplicates here
            let last_session_publishes = self.mqtt_state.borrow_mut().handle_reconnection();
            if let Some(publishes) = last_session_publishes {
                for publish in publishes{
                    let packet = Packet::Publish(publish);
                    sender = sender.send(packet).wait().unwrap();
                }
            }

            // receive incoming user request and write to network
            let mqtt_state = self.mqtt_state.clone();
            let commands_rx = commands_rx.by_ref();
            let network_reply_rx = network_reply_rx.by_ref();

            let mqtt_send = commands_rx.select(network_reply_rx)
            .map(move |msg| {
                mqtt_state.borrow_mut().handle_outgoing_mqtt_packet(msg).unwrap()
            }).map_err(|_| io::Error::new(ErrorKind::Other, "Error receiving client msg"))
              .forward(sender)
              .map(|_| ())
              .or_else(|_| { error!("Client send error"); future::ok(())});

            // join all the futures and run the reactor
            let mqtt_send_and_recv = mqtt_recv.join3(mqtt_send, ping_timer);
            if let Err(err) = self.reactor.run(mqtt_send_and_recv) {
                error!("Reactor halted. Error = {:?}", err);
            }
        }
    }


    /// Receives incoming mqtt packets and forwards them appropriately to user and network
    //  TODO: Remove box when `impl Future` is stable
    //  NOTE: Uses `unbounded` channel for sending notifications back to network as bounded
    //        channel clone will double the size of the queue anyway.
    fn mqtt_network_recv_future(&self, receiver: SplitStream<Framed<TlsStream<TcpStream>, MqttCodec>>, network_reply_tx: UnboundedSender<Packet>) -> Box<Future<Item=(), Error=io::Error>> {
        let mqtt_state = self.mqtt_state.clone();
        let notifier = self.notifier_tx.clone();
        
        let receiver = receiver.for_each(move |packet| {
            let (notification, reply) = match mqtt_state.borrow_mut().handle_incoming_mqtt_packet(packet) {
                Ok((notification, reply)) => (notification, reply),
                Err(e) => {
                    error!("{:?}", e);
                    (None, None)
                }
            };

            // send notification to user
            if let Some(notification) = notification {
                if let Err(e) = notifier.try_send(notification) {
                    error!("Publish notification send failed. Error = {:?}", e);
                }
            }

            // send reply back to network
            let network_reply_tx = network_reply_tx.clone();
            if let Some(reply) = reply {
                let s = network_reply_tx.send(reply).map(|_| ()).map_err(|_| io::Error::new(ErrorKind::Other, "Error receiving client msg"));
                Box::new(s) as Box<Future<Item=(), Error=io::Error>>
            } else {
                Box::new(future::ok(()))
            }
        });
        
        Box::new(receiver)
    }

    /// Sends ping to the network when client is idle
    fn ping_timer_future(&self, network_reply_tx: UnboundedSender<Packet>) -> Box<Future<Item=(), Error=io::Error>> {
        let handle = self.reactor.handle();
        let mqtt_state = self.mqtt_state.clone();

        if let Some(keep_alive) = self.opts.keep_alive {
            let interval = Interval::new(Duration::new(u64::from(keep_alive), 0), &handle).unwrap();
            let timer_future = interval.for_each(move |_t| {
                if mqtt_state.borrow().is_ping_required() {
                    debug!("Ping timer fire");
                    let network_reply_tx = network_reply_tx.clone();
                    let s = network_reply_tx.send(Packet::Pingreq).map(|_| ()).map_err(|_| io::Error::new(ErrorKind::Other, "Error receiving client msg"));
                    Box::new(s) as Box<Future<Item=(), Error=io::Error>>
                } else {
                    Box::new(future::ok(()))
                }
            });

            Box::new(timer_future)
        } else {
            Box::new(future::ok(()))
        }
    }


    /// Creates a mqtt connection on top of tcp/tls and returns a `Framed`
    fn mqtt_connect(&mut self) -> Result<Framed<TlsStream<TcpStream>, MqttCodec>, ConnectError> {
        let stream = self.mqtt_tls_connect();

        let response = self.reactor.run(stream);
        let (packet, frame) = response?;

        // Return `Framed` and previous session packets that are to be republished
        match packet.unwrap() {
            Packet::Connack(connack) => {
                self.mqtt_state.borrow_mut().handle_incoming_connack(connack)?;
                Ok(frame)
            }
            _ => unimplemented!(),
        }
    }

    // pub fn tcp_stream_future(&self) -> Box<Future<Item=(Option<Packet>, Framed<TcpStream, MqttCodec>), Error=io::Error>> {
    //     let addr: SocketAddr = self.opts.broker_addr.as_str().parse().unwrap();
    //     let handle = self.reactor.handle();
    //     let mqtt_state = self.mqtt_state.clone();
    //     let security = self.opts.security;

    //     let future_response = TcpStream::connect(&addr, &handle).and_then(move |connection| {
    //         let framed = connection.framed(MqttCodec);
    //         // let connect = mqtt_state.borrow_mut().handle_outgoing_connect();
    //         // let future_mqtt_connect = framed.send(Packet::Connect(connect));

    //         // future_mqtt_connect.and_then(|framed| {
    //         //     framed.into_future().and_then(|(res, stream)| Ok((res, stream))).map_err(|(err, _stream)| err)
    //         // })
    //         framed
    //     });

    //     Box::new(future_response)
    // }

    pub fn mqtt_tls_connect(&self) -> Box<Future<Item=(Option<Packet>, Framed<TlsStream<TcpStream>, MqttCodec>),  Error=io::Error>> {
        let addr = self.opts.broker_addr.clone();
        let domain = addr.split(":")
                         .map(str::to_string)
                         .next()
                         .unwrap_or_default();

        let mut addrs: Vec<_> = addr.to_socket_addrs().unwrap().collect();
        let addr = addrs.pop();
        let addr = match addr {
            Some(a) => a,
            None => {
                error!("Dns resolve array empty");
                return Box::new(future::err(io::Error::new(ErrorKind::AddrNotAvailable, "Dns resolution falied. Empty list")))
            }
        };

        println!("{:?}", addr);

        
        let handle = self.reactor.handle();
        // let security = self.opts.security;
        let mqtt_state = self.mqtt_state.clone();
        let socket = TcpStream::connect(&addr, &handle);
        let ca = self.get_ca_certificate();

        let tls_handshake = socket.and_then(move |socket| {
            let mut cx = TlsConnector::builder().unwrap();
            cx.add_root_certificate(ca).unwrap();
            let cx = cx.build().unwrap();

            let ip = &*format!("{}", &addr.ip());
            println!("{:?}", ip);
            let tls = cx.connect_async(&domain, socket);
            tls.map_err(|e| io::Error::new(io::ErrorKind::Other, e))
        });

        // mqtt connection to network
        let mqtt_request = tls_handshake.and_then(move |connection| {
            let framed = connection.framed(MqttCodec);
            let connect = mqtt_state.borrow_mut().handle_outgoing_connect();
            println!("{:?}",  connect);
            framed.send(Packet::Connect(connect))
        });

        // network reponse
        let mqtt_response = mqtt_request.and_then(|framed| {
            framed.into_future().and_then(|(res, stream)| Ok((res, stream))).map_err(|(err, _stream)| err)
        });

        Box::new(mqtt_response)
    }

    fn get_ca_certificate(&self) -> Certificate {
        Certificate::from_der(include_bytes!("/Users/raviteja/certs/roots.der")).unwrap()
    }
}