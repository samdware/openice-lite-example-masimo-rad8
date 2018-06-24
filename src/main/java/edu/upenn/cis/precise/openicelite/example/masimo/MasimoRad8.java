package edu.upenn.cis.precise.openicelite.example.masimo;

import edu.upenn.cis.precise.openicelite.iomt.api.DeviceInfo;
import edu.upenn.cis.precise.openicelite.iomt.api.IDriver;
import edu.upenn.cis.precise.openicelite.iomt.api.IDriverCallback;
import edu.upenn.cis.precise.openicelite.iomt.medical.masimo.rad8.MasimoDriver;
import edu.upenn.cis.precise.openicelite.middleware.api.IMiddleware;
import edu.upenn.cis.precise.openicelite.middleware.api.ShutdownHook;
import edu.upenn.cis.precise.openicelite.middleware.mqtt.Dongle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;

/**
 * Lightweight application to collect data from Masimo Pulse Oximeter and publish
 * data to OpenICE-lite MQTT broker
 * <p>
 * Available options for initialization:
 * - "device_port":         (required) serial port to connect to device
 *                              (e.g., ttyUSB0 on Ubuntu)
 * - "dongle_id":           dongle UUID to identify dongle and used as part of data topic
 * - "device_id":           device UUID to identify device connected to dongle
 *                              and used as part of data topic
 * - "device_type":         device description
 *                              (e.g., Masimo Rad-8)
 * - "project_name":        project name will be used as a base topic separation
 *                              and database file name (default to DEFAULT)
 * - "db_dir":              relative path to directory to storage database file
 * - "broker":              full address to the main broker
 *                              (e.g., ssl://hostname:port)
 * - "brokers":             fail-over addresses, separated with comma
 * - "connection_timeout":  timeout for connecting to broker
 * - "retry_interval":      retry interval if lost connection
 * - "alive_interval":      alive interval for MQTT connection
 * - "qos":                 MQTT QoS settings (0,1,2)
 * - "report_interval":     interval for which the dongle send report message to MapManager
 * - "username":            username to login broker
 * - "password":            password to login broker
 * - "report_interval":     interval for which the dongle send report message to MapManager
 * - "ca_cert_file":        absolute path to CA certification for TLS connection
 * - "client_cert_file":    absolute path to client certification for TLS connection
 *                              (if broker requires client cert)
 * - "client_key_file":     absolute path to client private key for TLS connection
 * - "key_password":        password to unlock client private key if needed
 *
 * @author Hung Nguyen (hungng@seas.upenn.edu)
 */
public class MasimoRad8 {
    private static final String DONGLE_ID = UUID.randomUUID().toString();
    private static final String DEVICE_ID = UUID.randomUUID().toString();
    private static final String PROPERTIES_FILE_NAME = "masimo.properties";

    private static final Logger logger = LogManager.getLogger(MasimoRad8.class);

    // Dongle configuration
    private final String dongleId;
    private DeviceInfo deviceInfo;
    private HashMap<String, Object> options;

    private IMiddleware middleware;
    private IDriver driver;

    public static void main(String[] args) {
        logger.info("Starting Masimo Dongle...");

        // Load configuration
        ClassLoader loader = MasimoRad8.class.getClassLoader();
        URL propResource = loader.getResource(PROPERTIES_FILE_NAME);
        File propFile = new File("./" + PROPERTIES_FILE_NAME);

        String devicePort = null;
        DeviceInfo deviceInfo = new DeviceInfo();
        deviceInfo.setDeviceId(DEVICE_ID);
        HashMap<String, Object> options = new HashMap<>();

        if (propFile.isFile() || propResource != null) {
            logger.info("Loading configuration from " +
                    (propFile.isFile() ? "./" : "default ") + PROPERTIES_FILE_NAME + "...");
            try (InputStream input = (propFile.isFile() ?
                    new FileInputStream("./" + PROPERTIES_FILE_NAME) :
                    loader.getResourceAsStream(PROPERTIES_FILE_NAME))) {
                Properties properties = new Properties();
                properties.load(input);

                // -- device port
                devicePort = properties.getProperty("device_port");
                logger.info("-- Device Port: " + devicePort);

                if (devicePort == null) throw new IllegalArgumentException("Invalid properties file!");

                // -- other properties
                Enumeration<?> p = properties.propertyNames();
                while (p.hasMoreElements()) {
                    String key = (String) p.nextElement();
                    String value = properties.getProperty(key);
                    options.put(key, value);
                }
            } catch (Exception ex) {
                logger.error("Failed to load configuration!", ex);
                System.exit(-1);
            }
        } else {
            logger.error("Cannot find configuration file - " + PROPERTIES_FILE_NAME + "!");
            System.exit(-1);
        }

        // Start Dongle
        MasimoRad8 dongle = new MasimoRad8(DONGLE_ID, deviceInfo, options);

        try {
            dongle.startMiddleware();
            Runtime.getRuntime().addShutdownHook(new ShutdownHook(dongle.middleware));
        } catch (Exception ex) {
            logger.error("Failed to start MQTT client!", ex);
            System.exit(-1);
        }

        try {
            dongle.startDriver(devicePort);
        } catch (Exception ex) {
            logger.error("Failed to start driver!", ex);
            System.exit(-1);
        }
    }

    /**
     * Masimo Dongle constructor
     *
     * @param dongleId dongle unique ID (UUID)
     * @param deviceInfo information of connected medical devices
     * @param options additional options to initialize MQTT dongle
     */
    public MasimoRad8(String dongleId, DeviceInfo deviceInfo, HashMap<String, Object> options) {
        this.dongleId = dongleId;
        this.deviceInfo = deviceInfo;
        this.options = options;
    }

    /**
     * Initialize middleware object, then connect to MQTT broker
     */
    public void startMiddleware() {
        middleware = new Dongle(dongleId);
        middleware.init(options);
        middleware.connect(null, null, null);
    }

    /**
     * Initialize driver to connect to Masimo device, then subscribe to data stream
     *
     * @param devicePort serial port for Masimo device (e.g., ttyUS0, ttyS0)
     */
    public void startDriver(String devicePort) {
        driver = new MasimoDriver(devicePort);
        DriverCallback callback = new DriverCallback(deviceInfo.getDeviceId(), middleware);
        driver.subscribe(null, callback);
        middleware.addDevice(deviceInfo);
    }

    private class DriverCallback implements IDriverCallback {
        private final String deviceId;
        private final IMiddleware middleware;
        private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");

        DriverCallback(String deviceId, IMiddleware middleware) {
            this.deviceId = deviceId;
            this.middleware = middleware;
        }

        /**
         * This demo doesn't need to handle byte array input
         *
         * @param message data to be handled
         */
        @Override
        public void handleMessage(byte[] message) { }

        /**
         * Handle new data from driver as a string
         *
         * @param message data to be handled
         */
        @Override
        public void handleMessage(String message) {
            long now = System.currentTimeMillis();
            JSONObject json = new JSONObject();
            json.put("time", dateFormat.format(now));
            json.put("epoch", new Long(now));
            json.put("data", message);

            middleware.publishId(deviceId, json.toString(), null);
        }
    }
}
