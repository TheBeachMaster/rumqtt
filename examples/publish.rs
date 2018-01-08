extern crate rumqtt;
extern crate pretty_env_logger;

use std::thread;
use std::time::Duration;

use rumqtt::{MqttOptions, ReconnectOptions, SecurityOptions, MqttClient, QoS};

fn main() {
    pretty_env_logger::init().unwrap();
    let security_opts = SecurityOptions::GcloudIotCore(("unused".to_owned(), "/Users/raviteja/certs/rsa_private.der".to_owned(), 60));
    let mqtt_opts = MqttOptions::new("projects/crested-return-122311/locations/us-central1/registries/iotcore-dev/devices/RAVI-MAC", "mqtt.googleapis.com:8883").unwrap()
                                .set_reconnect_opts(ReconnectOptions::Always(10))
                                .set_security_opts(security_opts);
    

    let (mut client, receiver) = MqttClient::start(mqtt_opts);

    thread::spawn(||{
        for i in receiver {
            println!("{:?}", i);
        }
    });

    for i in 0..1000 {
        if let Err(e) = client.publish("/devices/RAVI-MAC/events/imu", QoS::AtLeastOnce, vec![1, 2, 3]) {
            println!("{:?}", e);
        }
        thread::sleep(Duration::new(1, 0));
    }
    

    thread::sleep(Duration::new(60, 0));
}