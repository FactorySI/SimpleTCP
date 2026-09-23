using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace FactorySI.SimpleTcp
{
    /// <summary>
    /// Cliente TCP com recepção assíncrona e suporte a mensagens delimitadas.
    /// </summary>
    public class SimpleTcpClient : IDisposable
    {
        private const int TAMANHO_BUFFER_RECEBIMENTO = 8192;
        private const int DEFAULT_MAX_DELIMITER_MESSAGE_LENGTH = 1024 * 1024;

        private readonly SimpleTcpParam _param;
        private readonly object _stateLock = new object();
        private string _hostNameOrIpAddress;
        private int _port;
        private TcpClient _client;
        private TcpClient _connectingClient;
        private CancellationTokenSource _readCancellationTokenSource;
        private object _writeLock;
        private System.Text.Encoding _stringEncoder;
        private bool _disposed;
        private int _maxDelimiterMessageLength = DEFAULT_MAX_DELIMITER_MESSAGE_LENGTH;
        private int _writeTimeout;

        /// <summary>
        /// Inicializa uma nova instância do cliente TCP com codificação UTF-8 e delimitador padrão 0x13.
        /// </summary>
        public SimpleTcpClient()
        {
            StringEncoder = System.Text.Encoding.UTF8;
            Delimiter = 0x13;
        }

        /// <summary>
        /// Inicializa uma nova instância do cliente TCP com os parâmetros informados, codificação UTF-8 e delimitador padrão 0x13.
        /// </summary>
        public SimpleTcpClient(SimpleTcpParam param)
        {
            _param = param;
            StringEncoder = System.Text.Encoding.UTF8;
            Delimiter = 0x13;
        }

        /// <summary>
        /// Obtém ou define o byte que finaliza uma mensagem delimitada.
        /// </summary>
        public byte Delimiter { get; set; }

        /// <summary>
        /// Obtém ou define a codificação utilizada para converter mensagens de texto.
        /// Não aceita valores nulos.
        /// </summary>
        public System.Text.Encoding StringEncoder
        {
            get { return _stringEncoder; }
            set
            {
                if (value == null)
                {
                    throw new ArgumentNullException("value", "A codificação de texto deve ser informada.");
                }

                _stringEncoder = value;
            }
        }

        /// <summary>
        /// Obtém ou define a quantidade máxima de bytes aceitos em uma mensagem sem delimitador.
        /// O valor padrão é 1 MiB. A conexão atual é encerrada quando o limite é excedido.
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

        /// <summary>
        /// Obtém ou define o tempo máximo, em milissegundos, para uma escrita síncrona.
        /// O valor padrão é zero, que mantém o tempo limite infinito. Alterações durante uma conexão
        /// em andamento são aplicadas somente às novas conexões.
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
        /// Ocorre quando uma mensagem delimitada completa é recebida. O delimitador não faz parte de <see cref="Message.Data"/>.
        /// </summary>
        public event EventHandler<Message> DelimiterDataReceived;

        /// <summary>
        /// Ocorre após cada bloco bruto de bytes recebido pela conexão, inclusive quando ele contém mensagens delimitadas.
        /// </summary>
        public event EventHandler<Message> DataReceived;

        /// <summary>
        /// Obtém ou define se <see cref="Message.MessageString"/> remove espaços nas extremidades.
        /// </summary>
        public bool AutoTrimStrings { get; set; }

        /// <summary>
        /// Conecta o cliente ao host e porta informados.
        /// </summary>
        public SimpleTcpClient Connect(string hostNameOrIpAddress, int port)
        {
            if (hostNameOrIpAddress == null)
            {
                throw new ArgumentNullException("hostNameOrIpAddress", "O host ou endereço IP deve ser informado.");
            }

            if (string.IsNullOrWhiteSpace(hostNameOrIpAddress))
            {
                throw new ArgumentException("O host ou endereço IP deve ser informado.", "hostNameOrIpAddress");
            }

            if (port < 1 || port > 65535)
            {
                throw new ArgumentOutOfRangeException("port", "A porta deve estar entre 1 e 65535.");
            }

            TcpClient client = new TcpClient();
            client.SendTimeout = WriteTimeout;

            bool connectionAlreadyInProgress;
            lock (_stateLock)
            {
                ThrowIfDisposed();
                connectionAlreadyInProgress = _client != null || _connectingClient != null;
                if (!connectionAlreadyInProgress)
                {
                    _connectingClient = client;
                }
            }

            if (connectionAlreadyInProgress)
            {
                client.Close();
                throw new InvalidOperationException("O cliente TCP já está conectado ou possui uma conexão em andamento.");
            }

            CancellationTokenSource readCancellationTokenSource = null;
            try
            {
                client.Connect(hostNameOrIpAddress, port);

                readCancellationTokenSource = new CancellationTokenSource();
                object writeLock = new object();

                lock (_stateLock)
                {
                    if (_disposed)
                    {
                        throw new ObjectDisposedException(GetType().Name, "O cliente TCP já foi descartado.");
                    }

                    if (!ReferenceEquals(_connectingClient, client))
                    {
                        throw new InvalidOperationException("A conexão TCP foi cancelada antes de ser concluída.");
                    }

                    _connectingClient = null;
                    _client = client;
                    _readCancellationTokenSource = readCancellationTokenSource;
                    _writeLock = writeLock;
                    _hostNameOrIpAddress = hostNameOrIpAddress;
                    _port = port;
                }

                // ReadAsync trata internamente suas próprias falhas e encerra a sessão capturada.
                Task readTask = ReadAsync(client, readCancellationTokenSource, writeLock);

                return this;
            }
            catch
            {
                bool cancellationTokenSourcePublished = false;
                lock (_stateLock)
                {
                    if (ReferenceEquals(_connectingClient, client))
                    {
                        _connectingClient = null;
                    }

                    if (ReferenceEquals(_client, client))
                    {
                        cancellationTokenSourcePublished = ReferenceEquals(_readCancellationTokenSource, readCancellationTokenSource);
                        _client = null;
                        _readCancellationTokenSource = null;
                        _writeLock = null;
                    }
                }

                if (cancellationTokenSourcePublished)
                {
                    CancelAndClose(client, readCancellationTokenSource);
                }
                else
                {
                    CloseClient(client);
                    if (readCancellationTokenSource != null)
                    {
                        readCancellationTokenSource.Dispose();
                    }
                }

                throw;
            }
        }

        /// <summary>
        /// Envia uma linha e, se não houver resposta no tempo informado, reconecta usando o último host e porta conectados.
        /// </summary>
        public SimpleTcpClient ReconnectWriteLineAndGetReply(string data, TimeSpan timeout)
        {
            try
            {
                var retornaMensagem = WriteLineAndGetReply(data, timeout);
                if (retornaMensagem != null) return this;

                Disconnect();
                Connect(_hostNameOrIpAddress, _port);
                return this;
            }
            catch (Exception e)
            {
                Trace.TraceError("Falha ao reconectar o cliente TCP: " + e);
                return this;
            }
        }

        /// <summary>
        /// Envia bytes e, se não houver resposta no tempo informado, reconecta usando o último host e porta conectados.
        /// </summary>
        public SimpleTcpClient ReconnectWriteLineAndGetReply(byte[] data, TimeSpan timeout)
        {
            try
            {
                var retornaMensagem = WriteLineAndGetReply(data, timeout);
                if (retornaMensagem != null) return this;

                Disconnect();
                Connect(_hostNameOrIpAddress, _port);
                return this;
            }
            catch (Exception e)
            {
                Trace.TraceError("Falha ao reconectar o cliente TCP: " + e);
                return this;
            }
        }

        /// <summary>
        /// Cancela a leitura pendente e encerra a conexão TCP atual.
        /// </summary>
        public SimpleTcpClient Disconnect()
        {
            TcpClient client;
            TcpClient connectingClient;
            CancellationTokenSource cancellationTokenSource;

            lock (_stateLock)
            {
                ThrowIfDisposed();
                client = _client;
                connectingClient = _connectingClient;
                cancellationTokenSource = _readCancellationTokenSource;
                _client = null;
                _connectingClient = null;
                _readCancellationTokenSource = null;
                _writeLock = null;
            }

            CancelAndClose(client, cancellationTokenSource);
            if (!ReferenceEquals(connectingClient, client))
            {
                CloseClient(connectingClient);
            }

            return this;
        }

        /// <summary>
        /// Obtém o <see cref="System.Net.Sockets.TcpClient"/> da sessão conectada atual, ou <c>null</c> quando não há conexão.
        /// </summary>
        public TcpClient TcpClient
        {
            get
            {
                lock (_stateLock)
                {
                    ThrowIfDisposed();
                    return _client;
                }
            }
        }

        private async Task ReadAsync(TcpClient client, CancellationTokenSource cancellationTokenSource, object writeLock)
        {
            List<byte> delimiterMessageBuffer = null;
            CancellationToken cancellationToken = default(CancellationToken);

            try
            {
                byte[] receiveBuffer = new byte[TAMANHO_BUFFER_RECEBIMENTO];
                delimiterMessageBuffer = new List<byte>();
                cancellationToken = cancellationTokenSource.Token;
                NetworkStream stream = client.GetStream();
                while (!cancellationToken.IsCancellationRequested)
                {
                    int bytesRead = await stream.ReadAsync(receiveBuffer, 0, receiveBuffer.Length, cancellationToken).ConfigureAwait(false);
                    if (bytesRead == 0)
                    {
                        return;
                    }

                    if (!ProcessReceivedData(client, cancellationTokenSource, writeLock, receiveBuffer, bytesRead, delimiterMessageBuffer, cancellationToken))
                    {
                        return;
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
                    Trace.TraceError("Falha na leitura de dados do cliente TCP: " + ex);
                }
            }
            catch (SocketException ex)
            {
                if (!cancellationToken.IsCancellationRequested)
                {
                    Trace.TraceError("Falha na comunicação do cliente TCP: " + ex);
                }
            }
            catch (ObjectDisposedException ex)
            {
                if (!cancellationToken.IsCancellationRequested)
                {
                    Trace.TraceError("O cliente TCP já estava descartado: " + ex);
                }
            }
            catch (Exception ex)
            {
                Trace.TraceError("Falha inesperada na leitura assíncrona do cliente TCP: " + ex);
            }
            finally
            {
                if (delimiterMessageBuffer != null)
                {
                    delimiterMessageBuffer.Clear();
                }

                EncerrarSessao(client, cancellationTokenSource);
            }
        }

        private bool ProcessReceivedData(TcpClient client, CancellationTokenSource cancellationTokenSource, object writeLock, byte[] receiveBuffer, int bytesRead, List<byte> delimiterMessageBuffer, CancellationToken cancellationToken)
        {
            byte delimiter = Delimiter;
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
                    NotifyDelimiterMessageRx(client, writeLock, message);
                    continue;
                }

                delimiterMessageBuffer.Add(receivedByte);
                if (delimiterMessageBuffer.Count > MaxDelimiterMessageLength)
                {
                    delimiterMessageBuffer.Clear();
                    Trace.TraceError("Cliente TCP desconectado por exceder o limite de mensagem delimitada.");
                    EncerrarSessao(client, cancellationTokenSource);
                    return false;
                }
            }

            if (cancellationToken.IsCancellationRequested)
            {
                return false;
            }

            byte[] dataReceived = new byte[bytesRead];
            Array.Copy(receiveBuffer, dataReceived, bytesRead);
            NotifyEndTransmissionRx(client, writeLock, dataReceived);
            return !cancellationToken.IsCancellationRequested;
        }

        private void NotifyDelimiterMessageRx(TcpClient client, object writeLock, byte[] message)
        {
            EventHandler<Message> handler = DelimiterDataReceived;
            if (handler == null)
            {
                return;
            }

            try
            {
                handler(this, new Message(message, client, StringEncoder, Delimiter, AutoTrimStrings, data => WriteToSession(client, writeLock, data)));
            }
            catch (Exception ex)
            {
                Trace.TraceError("Falha no evento de mensagem delimitada do cliente TCP: " + ex);
            }
        }

        private void NotifyEndTransmissionRx(TcpClient client, object writeLock, byte[] message)
        {
            EventHandler<Message> handler = DataReceived;
            if (handler == null)
            {
                return;
            }

            try
            {
                handler(this, new Message(message, client, StringEncoder, Delimiter, AutoTrimStrings, data => WriteToSession(client, writeLock, data)));
            }
            catch (Exception ex)
            {
                Trace.TraceError("Falha no evento de dados recebidos do cliente TCP: " + ex);
            }
        }

        /// <summary>
        /// Transmite bytes pela conexão atual.
        /// </summary>
        public void Write(byte[] data)
        {
            if (data == null)
            {
                throw new ArgumentNullException("data");
            }

            TcpClient client;
            object writeLock;
            lock (_stateLock)
            {
                ThrowIfDisposed();
                client = _client;
                writeLock = _writeLock;
            }

            if (client == null || writeLock == null)
            {
                throw new InvalidOperationException("O cliente TCP não está conectado. Chame Connect antes de transmitir dados.");
            }

            WriteToSession(client, writeLock, data);
        }

        /// <summary>
        /// Transmite o texto informado pela conexão atual.
        /// </summary>
        public void Write(string data)
        {
            if (data == null)
            {
                return;
            }

            Write(StringEncoder.GetBytes(data));
        }

        /// <summary>
        /// Transmite o texto informado seguido do delimitador, quando ele ainda não estiver presente.
        /// Textos vazios geram um quadro delimitado vazio.
        /// </summary>
        public void WriteLine(string data)
        {
            if (data == null)
            {
                return;
            }

            byte[] payload = StringEncoder.GetBytes(data);
            if (payload.Length > 0 && payload[payload.Length - 1] == Delimiter)
            {
                Write(payload);
                return;
            }

            byte[] dataWithDelimiter = new byte[payload.Length + 1];
            Array.Copy(payload, dataWithDelimiter, payload.Length);
            dataWithDelimiter[dataWithDelimiter.Length - 1] = Delimiter;
            Write(dataWithDelimiter);
        }

        /// <summary>
        /// Envia uma linha e aguarda, de forma síncrona, o próximo bloco recebido até o tempo limite informado.
        /// </summary>
        public Message WriteLineAndGetReply(string data, TimeSpan timeout)
        {
            Message mReply = null;
            DataReceived += (s, e) => { mReply = e; };
            WriteLine(data);

            Stopwatch sw = new Stopwatch();
            sw.Start();
            while (mReply == null && sw.Elapsed < timeout)
            {
                Thread.Sleep(10);
            }

            return mReply;
        }

        /// <summary>
        /// Envia bytes e aguarda, de forma síncrona, o próximo bloco recebido até o tempo limite informado.
        /// </summary>
        public Message WriteLineAndGetReply(byte[] data, TimeSpan timeout)
        {
            Message mReply = null;
            DataReceived += (s, e) => { mReply = e; };
            Write(data);

            Stopwatch sw = new Stopwatch();
            sw.Start();
            while (mReply == null && sw.Elapsed < timeout)
            {
                Thread.Sleep(10);
            }

            return mReply;
        }

        private void WriteToSession(TcpClient client, object writeLock, byte[] data)
        {
            if (data == null)
            {
                throw new ArgumentNullException("data");
            }

            lock (_stateLock)
            {
                if (!ReferenceEquals(_client, client) || !ReferenceEquals(_writeLock, writeLock))
                {
                    throw new InvalidOperationException("A sessão TCP não está mais conectada.");
                }
            }

            lock (writeLock)
            {
                client.GetStream().Write(data, 0, data.Length);
            }
        }

        private void EncerrarSessao(TcpClient client, CancellationTokenSource cancellationTokenSource)
        {
            bool closeSession = false;
            lock (_stateLock)
            {
                if (ReferenceEquals(_client, client) && ReferenceEquals(_readCancellationTokenSource, cancellationTokenSource))
                {
                    _client = null;
                    _readCancellationTokenSource = null;
                    _writeLock = null;
                    closeSession = true;
                }
            }

            if (closeSession)
            {
                CancelAndClose(client, cancellationTokenSource);
            }
        }

        private static void CancelAndClose(TcpClient client, CancellationTokenSource cancellationTokenSource)
        {
            if (cancellationTokenSource != null)
            {
                try
                {
                    cancellationTokenSource.Cancel();
                }
                catch (ObjectDisposedException)
                {
                }
                catch (Exception ex)
                {
                    Trace.TraceError("Falha ao cancelar a leitura do cliente TCP: " + ex);
                }
                finally
                {
                    try
                    {
                        cancellationTokenSource.Dispose();
                    }
                    catch (ObjectDisposedException)
                    {
                    }
                }
            }

            CloseClient(client);
        }

        private static void CloseClient(TcpClient client)
        {
            if (client == null)
            {
                return;
            }

            try
            {
                client.Close();
            }
            catch (SocketException ex)
            {
                Trace.TraceError("Falha ao fechar o cliente TCP: " + ex);
            }
            catch (ObjectDisposedException ex)
            {
                Trace.TraceError("O cliente TCP já estava descartado ao fechar a conexão: " + ex);
            }
            catch (Exception ex)
            {
                Trace.TraceError("Falha inesperada ao fechar o cliente TCP: " + ex);
            }
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(GetType().Name, "O cliente TCP já foi descartado.");
            }
        }

        /// <summary>
        /// Libera a conexão TCP e os recursos gerenciados da instância.
        /// </summary>
        protected virtual void Dispose(bool disposing)
        {
            if (!disposing)
            {
                return;
            }

            TcpClient client;
            TcpClient connectingClient;
            CancellationTokenSource cancellationTokenSource;
            lock (_stateLock)
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;
                client = _client;
                connectingClient = _connectingClient;
                cancellationTokenSource = _readCancellationTokenSource;
                _client = null;
                _connectingClient = null;
                _readCancellationTokenSource = null;
                _writeLock = null;
            }

            CancelAndClose(client, cancellationTokenSource);
            if (!ReferenceEquals(connectingClient, client))
            {
                CloseClient(connectingClient);
            }
        }

        /// <summary>
        /// Libera a conexão TCP e impede novos usos desta instância.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }
    }
}
