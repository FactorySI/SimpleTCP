using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using FactorySI.SimpleTcp.Server;

namespace FactorySI.SimpleTcp
{
    public class SimpleTcpServer
    {
        public SimpleTcpServer()
        {
            Delimiter = 0x13;
            StringEncoder = System.Text.Encoding.UTF8;
        }

        private readonly object _listenersLock = new object();
        private readonly List<Server.ServerListener> _listeners = new List<Server.ServerListener>();
        private const int DEFAULT_MAX_DELIMITER_MESSAGE_LENGTH = 1024 * 1024;
        private int _maxDelimiterMessageLength = DEFAULT_MAX_DELIMITER_MESSAGE_LENGTH;
        private int _writeTimeout;
        public byte Delimiter { get; set; }
        public System.Text.Encoding StringEncoder { get; set; }
        public bool AutoTrimStrings { get; set; }

        /// <summary>
        /// Obtém ou define o tempo máximo, em milissegundos, para uma escrita síncrona em clientes aceitos pelo servidor.
        /// O valor padrão é zero, que mantém o tempo limite infinito.
        /// </summary>
        public int WriteTimeout
        {
            get { return _writeTimeout; }
            set
            {
                if (value < 0)
                {
                    throw new ArgumentOutOfRangeException("value", "O tempo limite de escrita deve ser maior ou igual a zero.");
                }

                _writeTimeout = value;
            }
        }

        /// <summary>
        /// Obtém ou define a quantidade máxima de bytes aceitos em uma mensagem sem delimitador.
        /// O valor padrão é 1 MiB. Clientes que excederem esse limite serão desconectados.
        /// </summary>
        public int MaxDelimiterMessageLength
        {
            get { return _maxDelimiterMessageLength; }
            set
            {
                if (value <= 0)
                {
                    throw new ArgumentOutOfRangeException("value", "O limite máximo de mensagem delimitada deve ser maior que zero.");
                }

                _maxDelimiterMessageLength = value;
            }
        }

        public event EventHandler<TcpClient> ClientConnected;
        public event EventHandler<TcpClient> ClientDisconnected;

        public event EventHandler<Message> DelimiterDataReceived;
        public event EventHandler<Message> DataReceived;

        public IEnumerable<IPAddress> GetIPAddresses()
        {
            List<IPAddress> ipAddresses = new List<IPAddress>();

            IEnumerable<NetworkInterface> enabledNetInterfaces = NetworkInterface.GetAllNetworkInterfaces()
                .Where(nic => nic.OperationalStatus == OperationalStatus.Up);
            foreach (NetworkInterface netInterface in enabledNetInterfaces)
            {
                IPInterfaceProperties ipProps = netInterface.GetIPProperties();
                foreach (UnicastIPAddressInformation addr in ipProps.UnicastAddresses)
                {
                    if (!ipAddresses.Contains(addr.Address))
                    {
                        ipAddresses.Add(addr.Address);
                    }
                }
            }

            var ipSorted = ipAddresses.OrderByDescending(ip => RankIpAddress(ip)).ToList();
            return ipSorted;
        }

        public List<IPAddress> GetListeningIPs()
        {
            List<IPAddress> listenIps = new List<IPAddress>();
            foreach (var l in GetListenersSnapshot())
            {
                if (!listenIps.Contains(l.IPAddress))
                {
                    listenIps.Add(l.IPAddress);
                }
            }

            return listenIps.OrderByDescending(ip => RankIpAddress(ip)).ToList();
        }

        public void Broadcast(byte[] data)
        {
            if (data == null)
            {
                throw new ArgumentNullException("data");
            }

            foreach (var listener in GetListenersSnapshot())
            {
                foreach (var client in listener.ConnectedClients)
                {
                    try
                    {
                        listener.WriteToClient(client, data);
                    }
                    catch (Exception ex)
                    {
                        System.Diagnostics.Trace.TraceError("Falha ao transmitir dados para um cliente TCP: " + ex);
                    }
                }
            }
        }

        public void Broadcast(string data)
        {
            if (data == null) { return; }
            Broadcast(StringEncoder.GetBytes(data));
        }

        public void BroadcastLine(string data)
        {
            if (string.IsNullOrEmpty(data)) { return; }
            if (data.LastOrDefault() != Delimiter)
            {
                Broadcast(data + StringEncoder.GetString(new byte[] { Delimiter }));
            }
            else
            {
                Broadcast(data);
            }
        }

        private int RankIpAddress(IPAddress addr)
        {
            int rankScore = 1000;

            if (IPAddress.IsLoopback(addr))
            {
                // rank loopback below others, even though their routing metrics may be better
                rankScore = 300;
            }
            else if (addr.AddressFamily == AddressFamily.InterNetwork)
            {
                rankScore += 100;
                // except...
                if (addr.GetAddressBytes().Take(2).SequenceEqual(new byte[] { 169, 254 }))
                {
                    // APIPA generated address - no router or DHCP server - to the bottom of the pile
                    rankScore = 0;
                }
            }

            if (rankScore > 500)
            {
                foreach (var nic in TryGetCurrentNetworkInterfaces())
                {
                    var ipProps = nic.GetIPProperties();
                    if (ipProps.GatewayAddresses.Any())
                    {
                        if (ipProps.UnicastAddresses.Any(u => u.Address.Equals(addr)))
                        {
                            // if the preferred NIC has multiple addresses, boost all equally
                            // (justifies not bothering to differentiate... IOW YAGNI)
                            rankScore += 1000;
                        }

                        // only considering the first NIC that is UP and has a gateway defined
                        break;
                    }
                }
            }

            return rankScore;
        }

        private static IEnumerable<NetworkInterface> TryGetCurrentNetworkInterfaces()
        {
            try
            {
                return NetworkInterface.GetAllNetworkInterfaces().Where(ni => ni.OperationalStatus == OperationalStatus.Up);
            }
            catch (NetworkInformationException)
            {
                return Enumerable.Empty<NetworkInterface>();
            }
        }

        public SimpleTcpServer Start(int port, bool ignoreNicsWithOccupiedPorts = true)
        {
            var ipSorted = GetIPAddresses();
            bool anyNicFailed = false;
            foreach (var ipAddr in ipSorted)
            {
                try
                {
                    Start(ipAddr, port);
                }
                catch (SocketException ex)
                {
                    DebugInfo(ex.ToString());
                    anyNicFailed = true;
                }
            }

            if (!IsStarted)
                throw new InvalidOperationException("Port was already occupied for all network interfaces");

            if (anyNicFailed && !ignoreNicsWithOccupiedPorts)
            {
                Stop();
                throw new InvalidOperationException("Port was already occupied for one or more network interfaces.");
            }

            return this;
        }

        public SimpleTcpServer Start(int port, AddressFamily addressFamilyFilter)
        {
            var ipSorted = GetIPAddresses().Where(ip => ip.AddressFamily == addressFamilyFilter);
            foreach (var ipAddr in ipSorted)
            {
                try
                {
                    Start(ipAddr, port);
                }
                catch { }
            }

            return this;
        }

        public bool IsStarted { get { return GetListenersSnapshot().Any(l => l.Listener.Active); } }

        public SimpleTcpServer Start(IPAddress ipAddress, int port)
        {
            lock (_listenersLock)
            {
                Server.ServerListener listener = new Server.ServerListener(this, ipAddress, port);
                _listeners.Add(listener);
            }

            return this;
        }

        public void Stop()
        {
            List<ServerListener> listeners;
            lock (_listenersLock)
            {
                listeners = new List<ServerListener>(_listeners);
                _listeners.Clear();
            }

            foreach (var listener in listeners)
            {
                listener.RequestStop();
            }
        }

        public List<ServerListener> GetClient()
        {
            return GetListenersSnapshot();
        }

        public int ConnectedClientsCount
        {
            get
            {
                return GetListenersSnapshot().Sum(l => l.ConnectedClientsCount);
            }
        }

        private List<ServerListener> GetListenersSnapshot()
        {
            lock (_listenersLock)
            {
                return new List<ServerListener>(_listeners);
            }
        }

        internal void NotifyDelimiterMessageRx(Server.ServerListener listener, TcpClient client, byte[] msg)
        {
            if (DelimiterDataReceived != null)
            {
                Message m = new Message(msg, client, StringEncoder, Delimiter, AutoTrimStrings, data => listener.WriteToClient(client, data));
                DelimiterDataReceived(this, m);
            }
        }


        internal void NotifyEndTransmissionRx(Server.ServerListener listener, TcpClient client, byte[] msg)
        {
            if (DataReceived != null)
            {
                Message m = new Message(msg, client, StringEncoder, Delimiter, AutoTrimStrings, data => listener.WriteToClient(client, data));
                DataReceived(this, m);
            }
        }

        internal void NotifyClientConnected(Server.ServerListener listener, TcpClient newClient)
        {
            if (ClientConnected != null)
            {
                ClientConnected(this, newClient);
            }
        }

        internal void NotifyClientDisconnected(Server.ServerListener listener, TcpClient disconnectedClient)
        {
            if (ClientDisconnected != null)
            {
                ClientDisconnected(this, disconnectedClient);
            }
        }



        #region Debug logging

        [System.Diagnostics.Conditional("DEBUG")]
        void DebugInfo(string format, params object[] args)
        {
            if (_debugInfoTime == null)
            {
                _debugInfoTime = new System.Diagnostics.Stopwatch();
                _debugInfoTime.Start();
            }
            System.Diagnostics.Debug.WriteLine(_debugInfoTime.ElapsedMilliseconds + ": " + format, args);
        }
        System.Diagnostics.Stopwatch _debugInfoTime;

        #endregion Debug logging
    }
}
