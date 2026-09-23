using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace FactorySI.SimpleTcp.Tests
{
    [TestClass]
    public class ClientAsyncLifecycleTests
    {
        [TestMethod]
        public async Task Fragmented_response_is_reassembled()
        {
            TcpListener listener = CriarListener();
            SimpleTcpClient client = new SimpleTcpClient();
            TaskCompletionSource<string> mensagemRecebida = new TaskCompletionSource<string>();

            try
            {
                client.DelimiterDataReceived += (sender, mensagem) => mensagemRecebida.TrySetResult(mensagem.MessageString);
                Task<TcpClient> acceptedTask = listener.AcceptTcpClientAsync();
                client.Connect(IPAddress.Loopback.ToString(), ObterPorta(listener));
                TcpClient serverClient = await Aguardar(acceptedTask);

                try
                {
                    Escrever(serverClient, "frag");
                    Escrever(serverClient, "mentada\x13");
                    Assert.AreEqual("fragmentada", await Aguardar(mensagemRecebida.Task));
                }
                finally
                {
                    serverClient.Close();
                }
            }
            finally
            {
                client.Dispose();
                listener.Stop();
            }
        }

        [TestMethod]
        public async Task Multiple_frames_in_one_read_are_raised_individually()
        {
            TcpListener listener = CriarListener();
            SimpleTcpClient client = new SimpleTcpClient();
            List<string> mensagens = new List<string>();
            TaskCompletionSource<bool> mensagensRecebidas = new TaskCompletionSource<bool>();

            try
            {
                client.DelimiterDataReceived += (sender, mensagem) =>
                {
                    lock (mensagens)
                    {
                        mensagens.Add(mensagem.MessageString);
                        if (mensagens.Count == 3)
                        {
                            mensagensRecebidas.TrySetResult(true);
                        }
                    }
                };

                Task<TcpClient> acceptedTask = listener.AcceptTcpClientAsync();
                client.Connect(IPAddress.Loopback.ToString(), ObterPorta(listener));
                TcpClient serverClient = await Aguardar(acceptedTask);

                try
                {
                    Escrever(serverClient, "primeira\x13segunda\x13terceira\x13");
                    await Aguardar(mensagensRecebidas.Task);
                    lock (mensagens)
                    {
                        CollectionAssert.AreEqual(new[] { "primeira", "segunda", "terceira" }, mensagens);
                    }
                }
                finally
                {
                    serverClient.Close();
                }
            }
            finally
            {
                client.Dispose();
                listener.Stop();
            }
        }

        [TestMethod]
        public async Task Delimiter_limit_closes_and_clears_current_session()
        {
            TcpListener listener = CriarListener();
            SimpleTcpClient client = new SimpleTcpClient { MaxDelimiterMessageLength = 4 };

            try
            {
                Task<TcpClient> acceptedTask = listener.AcceptTcpClientAsync();
                client.Connect(IPAddress.Loopback.ToString(), ObterPorta(listener));
                TcpClient serverClient = await Aguardar(acceptedTask);

                try
                {
                    Task<int> remoteRead = serverClient.GetStream().ReadAsync(new byte[1], 0, 1);
                    Escrever(serverClient, "12345");
                    Assert.AreEqual(0, await Aguardar(remoteRead));
                    await AguardarSessaoLimpa(client);
                    Assert.IsNull(client.TcpClient);
                }
                finally
                {
                    serverClient.Close();
                }
            }
            finally
            {
                client.Dispose();
                listener.Stop();
            }
        }

        [TestMethod]
        public async Task Disconnect_releases_pending_read_ten_times()
        {
            for (int tentativa = 0; tentativa < 10; tentativa++)
            {
                TcpListener listener = CriarListener();
                SimpleTcpClient client = new SimpleTcpClient();
                try
                {
                    Task<TcpClient> acceptedTask = listener.AcceptTcpClientAsync();
                    client.Connect(IPAddress.Loopback.ToString(), ObterPorta(listener));
                    TcpClient serverClient = await Aguardar(acceptedTask);
                    try
                    {
                        Task<int> remoteRead = serverClient.GetStream().ReadAsync(new byte[1], 0, 1);
                        client.Disconnect();
                        Assert.AreEqual(0, await Aguardar(remoteRead));
                    }
                    finally
                    {
                        serverClient.Close();
                    }
                }
                finally
                {
                    client.Dispose();
                    listener.Stop();
                }
            }
        }

        [TestMethod]
        public async Task Reconnection_does_not_mix_old_fragment()
        {
            TcpListener listener = CriarListener();
            SimpleTcpClient client = new SimpleTcpClient();
            TaskCompletionSource<string> mensagemRecebida = new TaskCompletionSource<string>();

            try
            {
                client.DelimiterDataReceived += (sender, mensagem) => mensagemRecebida.TrySetResult(mensagem.MessageString);
                Task<TcpClient> firstAcceptedTask = listener.AcceptTcpClientAsync();
                client.Connect(IPAddress.Loopback.ToString(), ObterPorta(listener));
                TcpClient firstServerClient = await Aguardar(firstAcceptedTask);

                try
                {
                    Escrever(firstServerClient, "antiga");
                    client.Disconnect();
                }
                finally
                {
                    firstServerClient.Close();
                }

                Task<TcpClient> secondAcceptedTask = listener.AcceptTcpClientAsync();
                client.Connect(IPAddress.Loopback.ToString(), ObterPorta(listener));
                TcpClient secondServerClient = await Aguardar(secondAcceptedTask);
                try
                {
                    Escrever(secondServerClient, "nova\x13");
                    Assert.AreEqual("nova", await Aguardar(mensagemRecebida.Task));
                }
                finally
                {
                    secondServerClient.Close();
                }
            }
            finally
            {
                client.Dispose();
                listener.Stop();
            }
        }

        [TestMethod]
        public async Task Duplicate_connect_fails_and_write_timeout_is_applied()
        {
            const int tempoLimite = 1500;
            TcpListener listener = CriarListener();
            SimpleTcpClient client = new SimpleTcpClient { WriteTimeout = tempoLimite };

            try
            {
                Task<TcpClient> acceptedTask = listener.AcceptTcpClientAsync();
                client.Connect(IPAddress.Loopback.ToString(), ObterPorta(listener));
                TcpClient serverClient = await Aguardar(acceptedTask);
                try
                {
                    Assert.AreEqual(tempoLimite, client.TcpClient.SendTimeout);
                    Assert.ThrowsExactly<InvalidOperationException>(() => client.Connect(IPAddress.Loopback.ToString(), ObterPorta(listener)));
                }
                finally
                {
                    serverClient.Close();
                }
            }
            finally
            {
                client.Dispose();
                listener.Stop();
            }
        }

        [TestMethod]
        public void Usage_after_dispose_fails()
        {
            SimpleTcpClient client = new SimpleTcpClient();
            client.Dispose();

            Assert.ThrowsExactly<ObjectDisposedException>(() => client.Connect("127.0.0.1", 1000));
            Assert.ThrowsExactly<ObjectDisposedException>(() => client.Disconnect());
            Assert.ThrowsExactly<ObjectDisposedException>(() => client.Write(new byte[0]));
            Assert.ThrowsExactly<ObjectDisposedException>(() => { var tcpClient = client.TcpClient; });
        }

        [TestMethod]
        public async Task Concurrent_writeline_and_reply_are_transmitted_as_complete_frames()
        {
            TcpListener listener = CriarListener();
            SimpleTcpClient client = new SimpleTcpClient();
            string mensagemEscrita = "W" + new string('w', 65536);
            string mensagemRespondida = "R" + new string('r', 65536);
            Task transmissaoConcluida = null;
            TaskCompletionSource<bool> respostaProcessada = new TaskCompletionSource<bool>();

            try
            {
                client.DelimiterDataReceived += (sender, mensagem) =>
                {
                    transmissaoConcluida = Task.Factory.StartNew(() => client.WriteLine(mensagemEscrita));
                    mensagem.Reply(Encoding.UTF8.GetBytes(mensagemRespondida + "\x13"));
                    respostaProcessada.TrySetResult(true);
                };

                Task<TcpClient> acceptedTask = listener.AcceptTcpClientAsync();
                client.Connect(IPAddress.Loopback.ToString(), ObterPorta(listener));
                TcpClient serverClient = await Aguardar(acceptedTask);
                try
                {
                    Escrever(serverClient, "pedido\x13");
                    await Aguardar(respostaProcessada.Task);
                    Assert.IsNotNull(transmissaoConcluida);
                    await Aguardar(transmissaoConcluida);

                    List<string> mensagens = await Aguardar(Task.Factory.StartNew(() => LerMensagensDelimitadas(serverClient, 2)));
                    CollectionAssert.AreEquivalent(new[] { mensagemEscrita, mensagemRespondida }, mensagens);
                }
                finally
                {
                    serverClient.Close();
                }
            }
            finally
            {
                client.Dispose();
                listener.Stop();
            }
        }

        private static TcpListener CriarListener()
        {
            TcpListener listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            return listener;
        }

        private static int ObterPorta(TcpListener listener)
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }

        private static void Escrever(TcpClient client, string data)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(data);
            client.GetStream().Write(bytes, 0, bytes.Length);
        }

        private static List<string> LerMensagensDelimitadas(TcpClient client, int quantidade)
        {
            byte[] buffer = new byte[8192];
            List<byte> dados = new List<byte>();
            List<string> mensagens = new List<string>();
            while (mensagens.Count < quantidade)
            {
                int bytesRead = client.GetStream().Read(buffer, 0, buffer.Length);
                Assert.IsTrue(bytesRead > 0, "A conexão foi encerrada antes de todas as mensagens serem recebidas.");
                for (int index = 0; index < bytesRead; index++)
                {
                    if (buffer[index] == 0x13)
                    {
                        mensagens.Add(Encoding.UTF8.GetString(dados.ToArray()));
                        dados.Clear();
                    }
                    else
                    {
                        dados.Add(buffer[index]);
                    }
                }
            }

            return mensagens;
        }

        private static async Task<T> Aguardar<T>(Task<T> task)
        {
            Task completedTask = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(5)));
            Assert.AreSame(task, completedTask, "A operação TCP não foi concluída dentro do tempo esperado.");
            return await task;
        }

        private static async Task Aguardar(Task task)
        {
            Task completedTask = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(5)));
            Assert.AreSame(task, completedTask, "A operação TCP não foi concluída dentro do tempo esperado.");
            await task;
        }

        private static async Task AguardarSessaoLimpa(SimpleTcpClient client)
        {
            for (int tentativa = 0; tentativa < 50; tentativa++)
            {
                if (client.TcpClient == null)
                {
                    return;
                }

                await Task.Delay(10);
            }

            Assert.Fail("A sessão TCP não foi limpa após o encerramento.");
        }
    }
}
