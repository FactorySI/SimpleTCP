using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace FactorySI.SimpleTcp.Server
{
    /// <summary>
    /// Mantém a escuta de um endereço TCP e o ciclo de leitura independente de cada cliente aceito.
    /// </summary>
    public class ServerListener
    {
        private const int MAXIMUM_RECEIVE_BUFFER_SIZE = 8192;

        private readonly TcpListenerEx _listener;
        private readonly List<TcpClient> _connectedClients = new List<TcpClient>();
        private readonly Dictionary<TcpClient, object> _writeLocks = new Dictionary<TcpClient, object>();
        private readonly SimpleTcpServer _parent;
        private readonly object _clientsLock = new object();
        private readonly CancellationTokenSource _stopCancellationTokenSource = new CancellationTokenSource();
        private bool _stopRequested;

        public int ConnectedClientsCount
        {
            get
            {
                lock (_clientsLock)
                {
                    return _connectedClients.Count;
                }
            }
        }

        public IEnumerable<TcpClient> ConnectedClients
        {
            get
            {
                lock (_clientsLock)
                {
                    return _connectedClients.ToArray();
                }
            }
        }

        internal ServerListener(SimpleTcpServer parentServer, IPAddress ipAddress, int port)
        {
            _parent = parentServer;
            IPAddress = ipAddress;
            Port = port;
            _listener = new TcpListenerEx(ipAddress, port);
            _listener.Start();

            Task acceptLoopTask = AcceptConnectionsAsync();
            if (acceptLoopTask.IsFaulted)
            {
                System.Diagnostics.Trace.TraceError("Falha ao iniciar a escuta assíncrona do servidor TCP: " + acceptLoopTask.Exception);
            }
        }

        internal IPAddress IPAddress { get; private set; }

        internal int Port { get; private set; }

        internal TcpListenerEx Listener { get { return _listener; } }

        /// <summary>
        /// Solicita a interrupção da escuta e fecha todos os clientes aceitos sem aguardar os loops de leitura.
        /// </summary>
        internal void RequestStop()
        {
            TcpClient[] clients;

            lock (_clientsLock)
            {
                if (_stopRequested)
                {
                    return;
                }

                _stopRequested = true;
                clients = _connectedClients.ToArray();
            }

            _stopCancellationTokenSource.Cancel();

            try
            {
                _listener.Stop();
            }
            catch (SocketException ex)
            {
                System.Diagnostics.Trace.TraceError("Falha ao interromper a escuta do servidor TCP: " + ex);
            }
            catch (ObjectDisposedException ex)
            {
                System.Diagnostics.Trace.TraceError("A escuta do servidor TCP já estava descartada: " + ex);
            }

            foreach (TcpClient client in clients)
            {
                FecharCliente(client, false);
            }
        }

        private async Task AcceptConnectionsAsync()
        {
            try
            {
                while (!_stopCancellationTokenSource.IsCancellationRequested)
                {
                    TcpClient client;

                    try
                    {
                        client = await _listener.AcceptTcpClientAsync().ConfigureAwait(false);
                    }
                    catch (ObjectDisposedException) when (_stopCancellationTokenSource.IsCancellationRequested)
                    {
                        break;
                    }
                    catch (SocketException) when (_stopCancellationTokenSource.IsCancellationRequested)
                    {
                        break;
                    }

                    client.SendTimeout = _parent.WriteTimeout;
                    if (!RegistrarCliente(client))
                    {
                        client.Close();
                        continue;
                    }

                    try
                    {
                        _parent.NotifyClientConnected(this, client);
                    }
                    catch (Exception ex)
                    {
                        System.Diagnostics.Trace.TraceError("Falha no evento de conexão do cliente TCP: " + ex);
                    }

                    if (ClienteEstaConectado(client) && !_stopCancellationTokenSource.IsCancellationRequested)
                    {
                        Task clientReadTask = ReadClientAsync(client, _stopCancellationTokenSource.Token);
                        if (clientReadTask.IsFaulted)
                        {
                            System.Diagnostics.Trace.TraceError("Falha ao iniciar a leitura assíncrona de um cliente TCP: " + clientReadTask.Exception);
                        }
                    }
                }
            }
            catch (ObjectDisposedException) when (_stopCancellationTokenSource.IsCancellationRequested)
            {
            }
            catch (SocketException ex)
            {
                if (!_stopCancellationTokenSource.IsCancellationRequested)
                {
                    System.Diagnostics.Trace.TraceError("Falha na escuta assíncrona do servidor TCP: " + ex);
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError("Falha inesperada na escuta assíncrona do servidor TCP: " + ex);
            }
        }

        private async Task ReadClientAsync(TcpClient client, CancellationToken cancellationToken)
        {
            byte[] receiveBuffer = new byte[MAXIMUM_RECEIVE_BUFFER_SIZE];
            List<byte> delimiterMessageBuffer = new List<byte>();

            try
            {
                NetworkStream stream = client.GetStream();

                while (!cancellationToken.IsCancellationRequested)
                {
                    int bytesRead = await stream.ReadAsync(receiveBuffer, 0, receiveBuffer.Length, cancellationToken).ConfigureAwait(false);
                    if (bytesRead == 0)
                    {
                        FecharCliente(client, true);
                        return;
                    }

                    try
                    {
                        if (!ProcessReceivedData(client, receiveBuffer, bytesRead, delimiterMessageBuffer, cancellationToken))
                        {
                            return;
                        }
                    }
                    catch (Exception ex)
                    {
                        System.Diagnostics.Trace.TraceError("Falha no processamento dos dados de um cliente TCP: " + ex);
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }
            catch (ObjectDisposedException) when (cancellationToken.IsCancellationRequested)
            {
            }
            catch (IOException ex)
            {
                if (!cancellationToken.IsCancellationRequested)
                {
                    System.Diagnostics.Trace.TraceError("Falha na leitura de dados de um cliente TCP: " + ex);
                    FecharCliente(client, true);
                }
            }
            catch (SocketException ex)
            {
                if (!cancellationToken.IsCancellationRequested)
                {
                    System.Diagnostics.Trace.TraceError("Falha na comunicação com cliente TCP: " + ex);
                    FecharCliente(client, true);
                }
            }
            catch (ObjectDisposedException ex)
            {
                if (!cancellationToken.IsCancellationRequested)
                {
                    System.Diagnostics.Trace.TraceError("Cliente TCP já estava descartado: " + ex);
                    FecharCliente(client, true);
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError("Falha inesperada na leitura de um cliente TCP: " + ex);
                FecharCliente(client, true);
            }
        }

        private bool ProcessReceivedData(
            TcpClient client,
            byte[] receiveBuffer,
            int bytesRead,
            List<byte> delimiterMessageBuffer,
            CancellationToken cancellationToken)
        {
            byte delimiter = _parent.Delimiter;

            for (int index = 0; index < bytesRead; index++)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return false;
                }

                byte receivedByte = receiveBuffer[index];
                if (receivedByte == delimiter)
                {
                    byte[] message = delimiterMessageBuffer.ToArray();
                    delimiterMessageBuffer.Clear();
                    _parent.NotifyDelimiterMessageRx(this, client, message);
                    continue;
                }

                delimiterMessageBuffer.Add(receivedByte);
                if (delimiterMessageBuffer.Count > _parent.MaxDelimiterMessageLength)
                {
                    System.Diagnostics.Trace.TraceError("Cliente TCP desconectado por exceder o limite de mensagem delimitada.");
                    FecharCliente(client, true);
                    return false;
                }
            }

            if (cancellationToken.IsCancellationRequested)
            {
                return false;
            }

            byte[] dataReceived = new byte[bytesRead];
            Array.Copy(receiveBuffer, dataReceived, bytesRead);
            _parent.NotifyEndTransmissionRx(this, client, dataReceived);
            return true;
        }

        private bool RegistrarCliente(TcpClient client)
        {
            lock (_clientsLock)
            {
                if (_stopRequested)
                {
                    return false;
                }

                _connectedClients.Add(client);
                _writeLocks.Add(client, new object());
                return true;
            }
        }

        private bool ClienteEstaConectado(TcpClient client)
        {
            lock (_clientsLock)
            {
                return _writeLocks.ContainsKey(client);
            }
        }

        private void FecharCliente(TcpClient client, bool notifyDisconnection)
        {
            bool shouldNotifyDisconnection;

            lock (_clientsLock)
            {
                if (!_connectedClients.Remove(client))
                {
                    return;
                }

                _writeLocks.Remove(client);
                shouldNotifyDisconnection = notifyDisconnection && !_stopRequested;
            }

            try
            {
                if (shouldNotifyDisconnection)
                {
                    _parent.NotifyClientDisconnected(this, client);
                }
            }
            finally
            {
                client.Close();
            }
        }

        internal void WriteToClient(TcpClient client, byte[] data)
        {
            if (data == null)
            {
                throw new ArgumentNullException("data");
            }

            object writeLock;
            lock (_clientsLock)
            {
                if (!_writeLocks.TryGetValue(client, out writeLock))
                {
                    throw new InvalidOperationException("O cliente TCP não está mais conectado ao servidor.");
                }
            }

            lock (writeLock)
            {
                client.GetStream().Write(data, 0, data.Length);
            }
        }
    }
}
