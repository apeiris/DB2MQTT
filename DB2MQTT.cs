using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Timers;
using System.Runtime.InteropServices;
using System.Data.SqlClient;
using System.Runtime.CompilerServices;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;
using MQTTnet.Extensions;
using MQTTnet.Extensions.ManagedClient;
using MQTTnet.Client.Connecting;
using MQTTnet.Client.Disconnecting;



namespace DB2MQTT {

    public enum ServiceState {
        SERVICE_STOPPED = 0x00000001,
        SERVICE_START_PENDING = 0x00000002,
        SERVICE_STOP_PENDING = 0x00000003,
        SERVICE_RUNNING = 0x00000004,
        SERVICE_CONTINUE_PENDING = 0x00000005,
        SERVICE_PAUSE_PENDING = 0x00000006,
        SERVICE_PAUSED = 0x00000007,
    }
    [StructLayout(LayoutKind.Sequential)]
    public struct ServiceStatus {
        public int dwServiceType;
        public ServiceState dwCurrentState;
        public int dwControlsAccepted;
        public int dwWin32ExitCode;
        public int dwServiceSpecificExitCode;
        public int dwCheckPoint;
        public int dwWaitHint;
    };
    public partial class DB2MQTT : ServiceBase {
        private int eventId = 1;

        #region native methods---------------------------------------------------
        [DllImport("advapi32.dll", SetLastError = true)]
        private static extern bool SetServiceStatus(System.IntPtr handle, ref ServiceStatus serviceStatus);
        #endregion native methods------------------------------------------------


        public DB2MQTT() {
            InitializeComponent();
            eventLog1 = new EventLog();
            if (!EventLog.SourceExists("DB2MQTT")) {
                EventLog.CreateEventSource("DB2MQTT", string.Empty);
                EventLog.CreateEventSource("DB2MQTT", "DB2MQTTLog");
            }
            eventLog1.Source = "DB2MQTT";
            eventLog1.Log = string.Empty;
        }
        private IManagedMqttClient qPub;

        private void Log(String s, EventLogEntryType x = EventLogEntryType.Information, [CallerMemberName] string cn = "", [CallerLineNumber] int ln = 0, [CallerFilePath] string fp = "") {
            switch (x) {
                case EventLogEntryType.Information:
                eventLog1.WriteEntry($"{DateTime.Now.ToString()}-{cn}@{fp.Substring(fp.LastIndexOf('\\') + 1)}:{ln}:{s}", EventLogEntryType.Information);
                break;
                case EventLogEntryType.Error:
                eventLog1.WriteEntry($"{DateTime.Now.ToString()}-{cn}@{fp.Substring(fp.LastIndexOf('\\') + 1)}:{ln}:{s}", EventLogEntryType.Error);
                break;
                case EventLogEntryType.Warning:
                eventLog1.WriteEntry($"{DateTime.Now.ToString()}-{cn}@{fp.Substring(fp.LastIndexOf('\\') + 1)}:{ln}:{s}", EventLogEntryType.Warning);
                break;
                case EventLogEntryType.FailureAudit:
                eventLog1.WriteEntry($"{DateTime.Now.ToString()}-{cn}@{fp.Substring(fp.LastIndexOf('\\') + 1)}:{ln}:{s}", EventLogEntryType.FailureAudit);
                break;
                case EventLogEntryType.SuccessAudit:
                eventLog1.WriteEntry($"{DateTime.Now.ToString()}-{cn}@{fp.Substring(fp.LastIndexOf('\\') + 1)}:{ln}:{s}", EventLogEntryType.SuccessAudit);
                break;
            }
        }

        public string GetEventAsPayload(string stmt) {
            using (SqlConnection con = new SqlConnection(Properties.Settings.Default.constr)) {
                con.Open();
                using (SqlCommand cmd = new SqlCommand(stmt, con)) {
                    return (String)cmd.ExecuteScalar();
                }
            }
        }
        private string GetNotificationPayload() {
            string mqttPayload = "";
            try {
                using (SqlConnection con = new SqlConnection(Properties.Settings.Default.constr)) {
                    con.Open();

                    using (SqlCommand cmd = new SqlCommand("select * from dbo.notifier", con)) {
                        Debug.WriteLine("Writeline @ ".PadRight(40, '-')); Debug.Indent();
                        using (SqlDataReader rdr = cmd.ExecuteReader(CommandBehavior.CloseConnection)) {
                            while (rdr.Read()) {
                                int EventID = rdr.GetInt32(1);
                                string stmt = Properties.Settings.Default.mqtt_PayloadSelectStatement;
                                stmt = stmt.Replace("{EventID}", EventID.ToString());
                                Debug.WriteLine($"{stmt}");
                                Debug.WriteLine($"EventID= {EventID}");
                                mqttPayload = GetEventAsPayload(stmt);
                                Log($"mqttPayload={mqttPayload}", EventLogEntryType.Information);
                            }
                        }
                        Debug.IndentLevel = 0;
                    }
                }

            } catch (Exception ex) {
                Log(ex.Message, EventLogEntryType.Error);
            }

            return mqttPayload;
        }


        private async Task PublishMessage(string sPayload) {

            Log(sPayload);

            //---- construct and send  mqtt message outof notification payload
      
            var payload = Encoding.UTF8.GetBytes(sPayload);
            var msg = new MqttApplicationMessageBuilder()
                .WithTopic(Properties.Settings.Default.mqtt_Topic)
                .WithPayload(payload)
                .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .WithRetainFlag(false)
                .Build();


            if (this.qPub != null) {
                Log($"Topic{msg.Topic} PayLoad:{sPayload} options:{qPub.Options.ClientOptions.ChannelOptions.ToString()}");
                await this.qPub.PublishAsync(msg);
            }

        }

        private void OnpublisherDisconnected(MqttClientDisconnectedEventArgs obj) {
            Log("Publisher Disconnected....");
        }
        private void onPublisherConnected(MqttClientConnectedEventArgs obj) {
            Log($"Publisher Connected...");
        }

        private async Task HandleReceivedAppMessage(MqttApplicationMessageReceivedEventArgs arg) {
            Log($"TS:{DateTime.Now:O} | Topic:{arg.ApplicationMessage.Topic} |Payload:{arg.ApplicationMessage.ConvertPayloadToString()}| QOS:{arg.ApplicationMessage.QualityOfServiceLevel}", EventLogEntryType.Information);
            await Task.Delay(1);
        }    
  
        private async void DbNotificationArrived(object sender, SqlNotificationEventArgs e) {
            string sMqPaylaod = GetNotificationPayload();// read from notification entry and then get Event data
            await PublishMessage(sMqPaylaod);
            RegisterDbNotificationListener();
        }


    


        #region Service DBNotificationLister Registration-----------------------------------

        private void RegisterDbNotificationListener() {
            try {
                SqlDependency.Start(Properties.Settings.Default.constr);
                using (SqlConnection con = new SqlConnection(Properties.Settings.Default.constr)) {
                    using (SqlCommand cmd = new SqlCommand(Properties.Settings.Default.NotifcationSelect, con) { CommandType = CommandType.Text }) {
                        con.Open();
                        var dep = new SqlDependency(cmd);
                        using (SqlDataReader rdr = cmd.ExecuteReader()) {
                            int i = 0;
                            while (rdr.Read()) {
                                Debug.Write($"i={i} : rdr.(0)= {rdr.GetInt32(0)} : rdr(1)= {rdr.GetInt32(1)} \n");
                                i++;
                            }
                        }
                        dep.OnChange += DbNotificationArrived;
                    }
                }
            } catch (Exception ex) {
                Log(ex.Message, EventLogEntryType.Error);
            }
        }

        private async Task Start() {
            try {
                Log("doing async start...");

                var mqttFactory = new MqttFactory();
              
                var tlsOptions = new MqttClientTlsOptions {
                    UseTls = false, IgnoreCertificateChainErrors = true, IgnoreCertificateRevocationErrors = true, AllowUntrustedCertificates = true
                };

                var options = new MqttClientOptions {
                    ClientId = "DB2MQTTPublisher",
                    ProtocolVersion = MQTTnet.Formatter.MqttProtocolVersion.V311,
                    ChannelOptions = new MqttClientTcpOptions {
                        Server = Properties.Settings.Default.mqtt_IP,Port = int.Parse(Properties.Settings.Default.mqtt_Port),
                        TlsOptions = tlsOptions
                    }
                };
                options.Credentials = new MqttClientCredentials {
                    Username = Properties.Settings.Default.mqtt_USER,Password = Encoding.UTF8.GetBytes(Properties.Settings.Default.mqtt_PASSWORD)
                };
                options.CleanSession = true;
                options.KeepAlivePeriod = TimeSpan.FromSeconds(60);

                this.qPub = mqttFactory.CreateManagedMqttClient();
                this.qPub.UseApplicationMessageReceivedHandler(this.HandleReceivedAppMessage);
                this.qPub.ConnectedHandler = new MqttClientConnectedHandlerDelegate(onPublisherConnected);
                this.qPub.DisconnectedHandler = new MqttClientDisconnectedHandlerDelegate(OnpublisherDisconnected);
                await this.qPub.StartAsync(new ManagedMqttClientOptions { ClientOptions = options });
            } catch (Exception ex) { Log($"{ex.Message}", EventLogEntryType.Error); }
        }
        protected override void OnStart(string[] args) {
            #region sevicerelated -------------------------------------------
            // Update the service state to Start Pending.
            ServiceStatus serviceStatus = new ServiceStatus();
            serviceStatus.dwCurrentState = ServiceState.SERVICE_START_PENDING;
            serviceStatus.dwWaitHint = 100000;
            SetServiceStatus(this.ServiceHandle, ref serviceStatus);
            Log("OnStart....");
            // Set up a timer that triggers every minute.
            Timer timer = new Timer();
            timer.Interval = 60000; // 60 seconds
            timer.Elapsed += new ElapsedEventHandler(this.OnTimer);
            timer.Start();
            // Update the service state to Running.
            serviceStatus.dwCurrentState = ServiceState.SERVICE_RUNNING;
            SetServiceStatus(this.ServiceHandle, ref serviceStatus);
            #endregion service related -----------------------------------------
          
            RegisterDbNotificationListener();
            _ = Start();
        }




        private void OnTimer(object sender, ElapsedEventArgs e) {
            eventLog1.WriteEntry($"Monitoring the system..{eventId}", EventLogEntryType.Information, eventId++);
        }

        protected override void OnStop() {
            eventLog1.WriteEntry("OnStop");
        }
        #endregion Service Routines-----------------------------------------------------------
    }
}
