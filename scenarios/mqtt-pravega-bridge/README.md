# MQTT to Pravega Bridge

This sample application reads events from MQTT and writes them to a Pravega stream.

## Components

- Pravega: Pravega provides a new storage abstraction - a stream - for continuous and unbounded data.
  A Pravega stream is a durable, elastic, append-only, unbounded sequence of bytes that has good performance and strong consistency.

  Pravega provides dynamic scaling that can increase and decrease parallelism to automatically respond
  to changes in the event rate.

  See <http://pravega.io> for more information.

- Pravega Video Demo: This is a simple command-line application that demonstrates how to write video files to Pravega.
  It also demonstrates how to read the video files from Pravega and decode them.

- Docker: This demo uses Docker and Docker Compose to greatly simplify the deployment of the various
  components on Linux and/or Windows servers, desktops, or even laptops.
  For more information, see <https://en.wikipedia.org/wiki/Docker_(software)>.

## Building and Running the Demo

### Install Java 8

```
apt-get install openjdk-8-jdk
```

### Install IntelliJ

Install from <https://www.jetbrains.com/idea>.
Enable the Lombok plugin.
Enable Annotations (settings -> build, execution, deployment, -> compiler -> annotation processors).

### Install Docker and Docker Compose

See <https://docs.docker.com/install/linux/docker-ce/ubuntu/>
and <https://docs.docker.com/compose/install/>.

### Run Pravega

This will run a development instance of Pravega locally.
In the command below, replace x.x.x.x with the IP address of a local network interface such as eth0.

```
cd
git clone https://github.com/pravega/pravega
cd pravega
export HOST_IP=x.x.x.x
docker-compose up -d
```

You can view the Pravega logs with `docker-compose logs --follow`.
You can view the stream files stored on HDFS with `docker-compose exec hdfs hdfs dfs -ls -h -R /`.

### Usage

- Install Mosquitto MQTT broker and clients.
  ```
  sudo apt-get install mosquitto mosquitto-clients
  ```

- If not automatically started, start Mosquitto broker.
  ```
  mosquitto
  ```

- Edit the file scenarios/mqtt-pravega-bridge/src/main/dist/conf
  to specify your Pravega controller URI (controllerUri) as
  `tcp://HOST_IP:9090`.

- In IntelliJ, run the class com.dell.mqtt.pravega.ApplicationMain with the following parameters:
  ```
  scenarios/mqtt-pravega-bridge/src/main/dist/conf
  ```

- Publish a sample MQTT message.
  ```
  mosquitto_pub -t center/0001 -m "12,34,56.78"
  ```

- You should see the following application output:
  ```
  [MQTT Call: CanDataReader] com.dell.mqtt.pravega.MqttListener: Writing Data Packet: CarID: 0001 Timestamp: 1551671403118 Payload: [B@2813d92f annotation: null
  ```
