extern crate rumqtt;
extern crate pretty_env_logger;
extern crate loggerv;

use std::thread;
use std::time::Duration;

use rumqtt::{MqttOptions, ReconnectOptions, SecurityOptions, MqttClient, QoS};

fn main() {
    loggerv::init_with_verbosity(0).unwrap();
    let security_opts = SecurityOptions::Tls(("ca.crt".to_owned(), "autobot.crt".to_owned(), "autobot.key".to_owned()));
    let mqtt_opts = MqttOptions::new("some_broker", "127.0.0.1:8889").unwrap()
                                .set_reconnect_opts(ReconnectOptions::Always(10))
                                .set_clean_session(false)
                                .set_security_opts(security_opts);
    

    let (mut client, receiver) = MqttClient::start(mqtt_opts);

    client.subscribe(vec![("hello/world", QoS::AtLeastOnce)]);

    thread::spawn(||{
        for i in receiver {
            println!("{:?}", i);
        }
    });

    for i in 0..1 {
       if let Err(e) = client.publish("hello/world", QoS::AtLeastOnce, vec![1, 2, 3]) {
           println!("{}", e);
       }
       thread::sleep_ms(100);
        // if let Err(e) = client.publish("/devices/RAVI-MAC/events/imu", QoS::AtLeastOnce, vec![1, 2, 3]) {
        //     println!("{:?}", e);
        // }
        // thread::sleep(Duration::new(1, 0));
    }
    

    thread::sleep(Duration::new(120, 0));
}