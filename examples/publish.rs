extern crate rumqtt;
extern crate loggerv;

use std::thread;
use std::time::Duration;

use rumqtt::{MqttOptions, ReconnectOptions, MqttClient, QoS};

fn main() {
    loggerv::init_with_verbosity(1).unwrap();
    let mqtt_opts = MqttOptions::new("projects/crested-return-122311/locations/us-central1/registries/iotcore-dev/devices/RAVI-LINUX", "mqtt.googleapis.com:8883").unwrap()
                                .set_reconnect_opts(ReconnectOptions::Always(10));
    

    let (mut client, receiver) = MqttClient::start(mqtt_opts);

    thread::spawn(||{
        for i in receiver {
            println!("{:?}", i);
        }
    });

    for i in 0..1000 {
        if let Err(e) = client.publish("hello/world", QoS::AtLeastOnce, vec![1, 2, 3]) {
            println!("{:?}", e);
        }
        thread::sleep(Duration::new(1, 0));
    }
    

    thread::sleep(Duration::new(60, 0));
}