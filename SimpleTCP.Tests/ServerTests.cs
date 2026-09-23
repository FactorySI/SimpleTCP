using System;
using System.Collections.Generic;
using System.Net;
using System.Linq;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTCP.Tests
{
	[TestClass]
	public class ServerTests : IDisposable
	{
		readonly int _serverPort = ObterPortaDisponivel();
		readonly SimpleTcpServer _server;

		public ServerTests()
		{
			_server = new SimpleTcpServer().Start(_serverPort);
        }


        public void Dispose()
		{
			if (_server.IsStarted)
				_server.Stop();
		}

		[TestMethod]
		public void Listening_port_opens_and_closes_when_server_starts_and_stops()
		{
			Assert.IsTrue(IsTcpPortListening(_serverPort), "Tcp port should be open when server has started.");
			_server.Stop();
			Assert.IsTrue(!IsTcpPortListening(_serverPort), "Tcp port should be closed when server has stopped.");
		}

		[TestMethod]
		public void Start_fails_if_all_nics_are_occupied()
		{
			Assert.ThrowsExactly<InvalidOperationException>(() =>
			{
				SimpleTcpServer server2 = null;
				try
				{
					server2 = new SimpleTcpServer().Start(_serverPort);
				}
				finally
				{
					if (server2 != null)
					{
						server2.Stop();
					}
				}
			});
		}



		public static bool IsTcpPortListening(int port)
		{
			return IPGlobalProperties.GetIPGlobalProperties()
				.GetActiveTcpListeners()
				.Where(x => x.Port == port)
				.Any();
		}

		private static int ObterPortaDisponivel()
		{
			var listener = new TcpListener(IPAddress.Loopback, 0);
			listener.Start();
			var porta = ((IPEndPoint)listener.LocalEndpoint).Port;
			listener.Stop();
			return porta;
		}
	}
}

namespace SimpleTCP.Tests
{
    [TestClass]
    public class ServerWritingTests
    {
        [TestMethod]
        public void WriteTimeout_rejects_negative_values()
        {
            var servidor = new SimpleTcpServer();

            Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => servidor.WriteTimeout = -1);
        }

        [TestMethod]
        public void WriteTimeout_is_applied_to_accepted_client()
        {
            const int tempoLimite = 1500;
            var clienteAceito = new ManualResetEvent(false);
            var porta = ObterPortaDisponivel();
            var servidor = new SimpleTcpServer { WriteTimeout = tempoLimite }.Start(IPAddress.Loopback, porta);
            TcpClient cliente = null;
            var tempoLimiteObservado = -1;

            try
            {
                servidor.ClientConnected += (sender, clienteServidor) =>
                {
                    tempoLimiteObservado = clienteServidor.SendTimeout;
                    clienteAceito.Set();
                };

                cliente = ConectarCliente(porta);

                Assert.IsTrue(clienteAceito.WaitOne(TimeSpan.FromSeconds(5)), "O servidor não aceitou o cliente dentro do tempo esperado.");
                Assert.AreEqual(tempoLimite, tempoLimiteObservado);
            }
            finally
            {
                if (cliente != null)
                {
                    cliente.Close();
                }

                servidor.Stop();
                clienteAceito.Close();
            }
        }

        [TestMethod]
        public void BroadcastLine_and_ReplyLine_send_complete_frames_when_called_concurrently()
        {
            var clienteAceito = new ManualResetEvent(false);
            var respostaEnviada = new ManualResetEvent(false);
            var liberarTransmissoes = new ManualResetEvent(false);
            var porta = ObterPortaDisponivel();
            var servidor = new SimpleTcpServer().Start(IPAddress.Loopback, porta);
            TcpClient cliente = null;
            Task transmissaoBroadcast = null;
            var mensagemBroadcast = "B" + new string('b', 262144);
            var mensagemResposta = "R" + new string('r', 262144);

            try
            {
                servidor.ClientConnected += (sender, clienteServidor) => clienteAceito.Set();
                servidor.DelimiterDataReceived += (sender, mensagem) =>
                {
                    transmissaoBroadcast = Task.Factory.StartNew(() =>
                    {
                        liberarTransmissoes.WaitOne();
                        servidor.BroadcastLine(mensagemBroadcast);
                    });

                    liberarTransmissoes.Set();
                    mensagem.ReplyLine(mensagemResposta);
                    respostaEnviada.Set();
                };

                cliente = ConectarCliente(porta);
                cliente.ReceiveTimeout = 5000;
                Assert.IsTrue(clienteAceito.WaitOne(TimeSpan.FromSeconds(5)), "O servidor não aceitou o cliente dentro do tempo esperado.");

                Task<List<string>> leituraMensagens = Task.Factory.StartNew(() => LerMensagensDelimitadas(cliente, 2));
                Escrever(cliente, "enviar\x13");

                Assert.IsTrue(respostaEnviada.WaitOne(TimeSpan.FromSeconds(5)), "O evento do servidor não concluiu a resposta dentro do tempo esperado.");
                Assert.IsNotNull(transmissaoBroadcast, "A transmissão de broadcast não foi iniciada.");
                Assert.IsTrue(transmissaoBroadcast.Wait(TimeSpan.FromSeconds(5)), "A transmissão de broadcast não foi concluída dentro do tempo esperado.");

                Assert.IsTrue(leituraMensagens.Wait(TimeSpan.FromSeconds(5)), "O cliente não recebeu as duas mensagens dentro do tempo esperado.");
                List<string> mensagensRecebidas = leituraMensagens.Result;
                CollectionAssert.AreEquivalent(new[] { mensagemBroadcast, mensagemResposta }, mensagensRecebidas);
            }
            finally
            {
                if (cliente != null)
                {
                    cliente.Close();
                }

                servidor.Stop();
                clienteAceito.Close();
                respostaEnviada.Close();
                liberarTransmissoes.Close();
            }
        }

        private static List<string> LerMensagensDelimitadas(TcpClient cliente, int quantidadeEsperada)
        {
            var dadosRecebidos = new List<byte>();
            var mensagens = new List<string>();
            var buffer = new byte[8192];

            while (mensagens.Count < quantidadeEsperada)
            {
                int quantidadeLida = cliente.GetStream().Read(buffer, 0, buffer.Length);
                Assert.IsTrue(quantidadeLida > 0, "A conexão foi fechada antes de todas as mensagens serem recebidas.");

                for (int indice = 0; indice < quantidadeLida; indice++)
                {
                    if (buffer[indice] == 0x13)
                    {
                        mensagens.Add(Encoding.UTF8.GetString(dadosRecebidos.ToArray()));
                        dadosRecebidos.Clear();
                    }
                    else
                    {
                        dadosRecebidos.Add(buffer[indice]);
                    }
                }
            }

            return mensagens;
        }

        private static TcpClient ConectarCliente(int porta)
        {
            var cliente = new TcpClient();
            cliente.Connect(IPAddress.Loopback, porta);
            return cliente;
        }

        private static void Escrever(TcpClient cliente, string dados)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(dados);
            cliente.GetStream().Write(bytes, 0, bytes.Length);
        }

        private static int ObterPortaDisponivel()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var porta = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return porta;
        }
    }
}

namespace SimpleTCP.Tests
{
    [TestClass]
    public class ServerAsyncLifecycleTests
    {
        [TestMethod]
        public void Idle_clients_do_not_delay_an_active_client()
        {
            var mensagemRecebida = new ManualResetEvent(false);
            var porta = ObterPortaDisponivel();
            var servidor = new SimpleTcpServer().Start(IPAddress.Loopback, porta);
            var clientesOciosos = new List<TcpClient>();
            TcpClient clienteAtivo = null;

            try
            {
                servidor.DataReceived += (sender, mensagem) =>
                {
                    if (mensagem.MessageString == "ativo")
                    {
                        mensagemRecebida.Set();
                    }
                };

                for (int indice = 0; indice < 12; indice++)
                {
                    clientesOciosos.Add(ConectarCliente(porta));
                }

                clienteAtivo = ConectarCliente(porta);
                Escrever(clienteAtivo, "ativo");

                Assert.IsTrue(mensagemRecebida.WaitOne(TimeSpan.FromSeconds(2)), "O cliente ativo foi atrasado pelos clientes ociosos.");
            }
            finally
            {
                foreach (TcpClient clienteOcioso in clientesOciosos)
                {
                    clienteOcioso.Close();
                }

                if (clienteAtivo != null)
                {
                    clienteAtivo.Close();
                }

                servidor.Stop();
                mensagemRecebida.Close();
            }
        }

        [TestMethod]
        public void Simultaneous_clients_preserve_messages_per_connection()
        {
            const int quantidadeClientes = 8;
            const int quantidadeMensagensPorCliente = 20;
            var mensagensPorCliente = new Dictionary<string, List<string>>();
            var mensagensRecebidas = new ManualResetEvent(false);
            var porta = ObterPortaDisponivel();
            var servidor = new SimpleTcpServer().Start(IPAddress.Loopback, porta);
            var clientes = new List<TcpClient>();

            try
            {
                servidor.DelimiterDataReceived += (sender, mensagem) =>
                {
                    string cliente = mensagem.TcpClient.Client.RemoteEndPoint.ToString();
                    lock (mensagensPorCliente)
                    {
                        List<string> mensagens;
                        if (!mensagensPorCliente.TryGetValue(cliente, out mensagens))
                        {
                            mensagens = new List<string>();
                            mensagensPorCliente.Add(cliente, mensagens);
                        }

                        mensagens.Add(mensagem.MessageString);
                        if (mensagensPorCliente.Count == quantidadeClientes && mensagensPorCliente.All(item => item.Value.Count == quantidadeMensagensPorCliente))
                        {
                            mensagensRecebidas.Set();
                        }
                    }
                };

                for (int indice = 0; indice < quantidadeClientes; indice++)
                {
                    clientes.Add(ConectarCliente(porta));
                }

                Task[] transmissoes = clientes.Select((cliente, indice) => Task.Factory.StartNew(() =>
                {
                    for (int mensagem = 0; mensagem < quantidadeMensagensPorCliente; mensagem++)
                    {
                        Escrever(cliente, indice + "-" + mensagem + "\x13");
                    }
                })).ToArray();

                Assert.IsTrue(Task.WaitAll(transmissoes, TimeSpan.FromSeconds(5)), "Os clientes não concluíram as transmissões simultâneas.");
                Assert.IsTrue(mensagensRecebidas.WaitOne(TimeSpan.FromSeconds(5)), "O servidor não recebeu todas as mensagens simultâneas.");

                lock (mensagensPorCliente)
                {
                    foreach (TcpClient cliente in clientes)
                    {
                        string identificadorCliente = cliente.Client.LocalEndPoint.ToString();
                        CollectionAssert.AreEqual(
                            Enumerable.Range(0, quantidadeMensagensPorCliente).Select(mensagem => clientes.IndexOf(cliente) + "-" + mensagem).ToArray(),
                            mensagensPorCliente[identificadorCliente]);
                    }
                }
            }
            finally
            {
                foreach (TcpClient cliente in clientes)
                {
                    cliente.Close();
                }

                servidor.Stop();
                mensagensRecebidas.Close();
            }
        }

        [TestMethod]
        public void Stop_releases_pending_reads_and_closes_the_port()
        {
            for (int tentativa = 0; tentativa < 10; tentativa++)
            {
                var clienteAceito = new ManualResetEvent(false);
                var porta = ObterPortaDisponivel();
                var servidor = new SimpleTcpServer().Start(IPAddress.Loopback, porta);
                TcpClient cliente = null;

                try
                {
                    servidor.ClientConnected += (sender, clienteServidor) => clienteAceito.Set();
                    cliente = ConectarCliente(porta);
                    Assert.IsTrue(clienteAceito.WaitOne(TimeSpan.FromSeconds(5)), "O servidor não aceitou o cliente ocioso.");

                    Task<int> leituraPendente = cliente.GetStream().ReadAsync(new byte[1], 0, 1);
                    servidor.Stop();

                    Assert.IsTrue(leituraPendente.Wait(TimeSpan.FromSeconds(5)), "Stop não liberou a leitura pendente do cliente.");
                    Assert.IsFalse(IsTcpPortListening(porta), "A porta permaneceu em escuta após Stop.");
                    Assert.AreEqual(0, leituraPendente.Result, "O cliente deveria observar o encerramento remoto após Stop.");
                }
                finally
                {
                    if (cliente != null)
                    {
                        cliente.Close();
                    }

                    servidor.Stop();
                    clienteAceito.Close();
                }
            }
        }

        [TestMethod]
        public void Fragmented_and_multiple_frames_are_processed_in_order()
        {
            var mensagensRecebidas = new List<string>();
            var mensagensConcluidas = new ManualResetEvent(false);
            var porta = ObterPortaDisponivel();
            var servidor = new SimpleTcpServer().Start(IPAddress.Loopback, porta);
            TcpClient cliente = null;

            try
            {
                servidor.DelimiterDataReceived += (sender, mensagem) =>
                {
                    lock (mensagensRecebidas)
                    {
                        mensagensRecebidas.Add(mensagem.MessageString);
                        if (mensagensRecebidas.Count == 3)
                        {
                            mensagensConcluidas.Set();
                        }
                    }
                };

                cliente = ConectarCliente(porta);
                Escrever(cliente, "frag");
                Escrever(cliente, "mentada\x13segunda\x13terceira\x13");

                Assert.IsTrue(mensagensConcluidas.WaitOne(TimeSpan.FromSeconds(5)), "Os frames fragmentados não foram processados.");
                lock (mensagensRecebidas)
                {
                    CollectionAssert.AreEqual(new[] { "fragmentada", "segunda", "terceira" }, mensagensRecebidas);
                }
            }
            finally
            {
                if (cliente != null)
                {
                    cliente.Close();
                }

                servidor.Stop();
                mensagensConcluidas.Close();
            }
        }

        [TestMethod]
        public void Client_disconnection_is_not_notified_twice()
        {
            var clienteDesconectado = new ManualResetEvent(false);
            var quantidadeDesconexoes = 0;
            var porta = ObterPortaDisponivel();
            var servidor = new SimpleTcpServer().Start(IPAddress.Loopback, porta);
            TcpClient cliente = null;

            try
            {
                servidor.ClientDisconnected += (sender, clienteServidor) =>
                {
                    if (Interlocked.Increment(ref quantidadeDesconexoes) == 1)
                    {
                        clienteDesconectado.Set();
                    }
                };

                cliente = ConectarCliente(porta);
                cliente.Close();

                Assert.IsTrue(clienteDesconectado.WaitOne(TimeSpan.FromSeconds(5)), "O servidor não notificou a desconexão remota.");
                Assert.AreEqual(1, Volatile.Read(ref quantidadeDesconexoes), "ClientDisconnected foi disparado mais de uma vez.");
            }
            finally
            {
                servidor.Stop();
                clienteDesconectado.Close();
            }
        }

        private static TcpClient ConectarCliente(int porta)
        {
            var cliente = new TcpClient();
            cliente.Connect(IPAddress.Loopback, porta);
            return cliente;
        }

        private static void Escrever(TcpClient cliente, string dados)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(dados);
            cliente.GetStream().Write(bytes, 0, bytes.Length);
        }

        private static bool IsTcpPortListening(int porta)
        {
            return IPGlobalProperties.GetIPGlobalProperties().GetActiveTcpListeners().Any(endpoint => endpoint.Port == porta);
        }

        private static int ObterPortaDisponivel()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var porta = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return porta;
        }
    }
}

namespace SimpleTCP.Tests
{
    [TestClass]
    public class ServerMessageReceptionTests
    {
        [TestMethod]
        public void Delimited_messages_from_two_clients_use_isolated_buffers()
        {
            var mensagensRecebidas = new Dictionary<string, string>();
            var dadosParciaisRecebidos = new AutoResetEvent(false);
            var mensagensRecebidasEvent = new ManualResetEvent(false);
            var porta = ObterPortaDisponivel();
            var servidor = new SimpleTcpServer().Start(IPAddress.Loopback, porta);
            TcpClient primeiroCliente = null;
            TcpClient segundoCliente = null;

            try
            {
                servidor.DataReceived += (sender, mensagem) =>
                {
                    if (mensagem.MessageString == "primeira")
                    {
                        dadosParciaisRecebidos.Set();
                    }
                };
                servidor.DelimiterDataReceived += (sender, mensagem) =>
                {
                    lock (mensagensRecebidas)
                    {
                        mensagensRecebidas[mensagem.TcpClient.Client.RemoteEndPoint.ToString()] = mensagem.MessageString;
                        if (mensagensRecebidas.Count == 2)
                        {
                            mensagensRecebidasEvent.Set();
                        }
                    }
                };

                primeiroCliente = ConectarCliente(porta);
                segundoCliente = ConectarCliente(porta);

                Escrever(primeiroCliente, "primeira");
                Assert.IsTrue(dadosParciaisRecebidos.WaitOne(TimeSpan.FromSeconds(5)), "O servidor não recebeu os dados parciais do primeiro cliente.");

                Escrever(segundoCliente, "segunda\x13");
                Escrever(primeiroCliente, "\x13");

                Assert.IsTrue(mensagensRecebidasEvent.WaitOne(TimeSpan.FromSeconds(5)), "O servidor não recebeu as duas mensagens delimitadas.");

                lock (mensagensRecebidas)
                {
                    Assert.AreEqual("primeira", mensagensRecebidas[primeiroCliente.Client.LocalEndPoint.ToString()]);
                    Assert.AreEqual("segunda", mensagensRecebidas[segundoCliente.Client.LocalEndPoint.ToString()]);
                }
            }
            finally
            {
                if (primeiroCliente != null)
                {
                    primeiroCliente.Close();
                }

                if (segundoCliente != null)
                {
                    segundoCliente.Close();
                }

                servidor.Stop();
                dadosParciaisRecebidos.Close();
                mensagensRecebidasEvent.Close();
            }
        }

        [TestMethod]
        public void Multiple_delimited_messages_in_one_write_are_raised_individually()
        {
            var mensagensRecebidas = new List<string>();
            var mensagensRecebidasEvent = new ManualResetEvent(false);
            var porta = ObterPortaDisponivel();
            var servidor = new SimpleTcpServer().Start(IPAddress.Loopback, porta);
            TcpClient cliente = null;

            try
            {
                servidor.DelimiterDataReceived += (sender, mensagem) =>
                {
                    lock (mensagensRecebidas)
                    {
                        mensagensRecebidas.Add(mensagem.MessageString);
                        if (mensagensRecebidas.Count == 2)
                        {
                            mensagensRecebidasEvent.Set();
                        }
                    }
                };

                cliente = ConectarCliente(porta);
                Escrever(cliente, "primeira\x13segunda\x13");

                Assert.IsTrue(mensagensRecebidasEvent.WaitOne(TimeSpan.FromSeconds(5)), "O servidor não separou as duas mensagens delimitadas.");

                lock (mensagensRecebidas)
                {
                    CollectionAssert.AreEqual(new[] { "primeira", "segunda" }, mensagensRecebidas);
                }
            }
            finally
            {
                if (cliente != null)
                {
                    cliente.Close();
                }

                servidor.Stop();
                mensagensRecebidasEvent.Close();
            }
        }

        [TestMethod]
        public void GetClient_returns_a_copy_of_the_listener_list()
        {
            var porta = ObterPortaDisponivel();
            var servidor = new SimpleTcpServer().Start(IPAddress.Loopback, porta);

            try
            {
                List<SimpleTCP.Server.ServerListener> listeners = servidor.GetClient();
                listeners.Clear();

                Assert.AreEqual(1, servidor.GetClient().Count, "A alteração da lista retornada não deve alterar os listeners internos do servidor.");
                Assert.IsTrue(servidor.IsStarted, "O servidor deve continuar em execução após alterar a cópia retornada.");
            }
            finally
            {
                servidor.Stop();
            }
        }

        [TestMethod]
        public void Stop_called_from_data_received_does_not_deadlock()
        {
            var paradaConcluida = new ManualResetEvent(false);
            var porta = ObterPortaDisponivel();
            var servidor = new SimpleTcpServer().Start(IPAddress.Loopback, porta);
            TcpClient cliente = null;

            try
            {
                servidor.DataReceived += (sender, mensagem) =>
                {
                    servidor.Stop();
                    paradaConcluida.Set();
                };

                cliente = ConectarCliente(porta);
                Escrever(cliente, "parar");

                Assert.IsTrue(paradaConcluida.WaitOne(TimeSpan.FromSeconds(5)), "Stop chamado no evento não foi concluído dentro do tempo esperado.");
                Assert.IsFalse(servidor.IsStarted, "A porta deve estar fechada quando Stop retornar.");
            }
            finally
            {
                if (cliente != null)
                {
                    cliente.Close();
                }

                servidor.Stop();
                paradaConcluida.Close();
            }
        }

        [TestMethod]
        public void Client_exceeding_delimited_message_limit_is_disconnected_without_affecting_another_client()
        {
            var clienteProblemaDesconectado = new ManualResetEvent(false);
            var mensagemClienteValidoRecebida = new ManualResetEvent(false);
            var porta = ObterPortaDisponivel();
            var servidor = new SimpleTcpServer
            {
                MaxDelimiterMessageLength = 4
            }.Start(IPAddress.Loopback, porta);
            TcpClient clienteProblema = null;
            TcpClient clienteValido = null;

            try
            {
                servidor.ClientDisconnected += (sender, cliente) =>
                {
                    if (clienteProblema != null && cliente.Client.RemoteEndPoint.ToString() == clienteProblema.Client.LocalEndPoint.ToString())
                    {
                        clienteProblemaDesconectado.Set();
                    }
                };
                servidor.DelimiterDataReceived += (sender, mensagem) =>
                {
                    if (mensagem.MessageString == "ok")
                    {
                        mensagemClienteValidoRecebida.Set();
                    }
                };

                clienteProblema = ConectarCliente(porta);
                clienteValido = ConectarCliente(porta);

                Escrever(clienteProblema, "12345");
                Assert.IsTrue(clienteProblemaDesconectado.WaitOne(TimeSpan.FromSeconds(5)), "O cliente que excedeu o limite não foi desconectado.");

                Escrever(clienteValido, "ok\x13");
                Assert.IsTrue(mensagemClienteValidoRecebida.WaitOne(TimeSpan.FromSeconds(5)), "O cliente válido foi afetado pela desconexão do cliente problemático.");
            }
            finally
            {
                if (clienteProblema != null)
                {
                    clienteProblema.Close();
                }

                if (clienteValido != null)
                {
                    clienteValido.Close();
                }

                servidor.Stop();
                clienteProblemaDesconectado.Close();
                mensagemClienteValidoRecebida.Close();
            }
        }

        private static TcpClient ConectarCliente(int porta)
        {
            var cliente = new TcpClient();
            cliente.Connect(IPAddress.Loopback, porta);
            return cliente;
        }

        private static void Escrever(TcpClient cliente, string dados)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(dados);
            cliente.GetStream().Write(bytes, 0, bytes.Length);
        }

        private static int ObterPortaDisponivel()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var porta = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return porta;
        }
    }
}
