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
    public class ClientRequestReplyTests
    {
        private readonly List<TcpListener> _listeners = new List<TcpListener>();

        [TestCleanup]
        public void EncerrarListeners()
        {
            foreach (TcpListener listener in _listeners)
            {
                listener.Stop();
            }
        }

        [TestMethod]
        public async Task WriteLineAndGetReplyAsync_recebe_resposta_fragmentada_sem_delimitador()
        {
            TcpListener listener = CriarListener();
            using (SimpleTcpClient client = new SimpleTcpClient())
            {
                Task<TcpClient> aceita = listener.AcceptTcpClientAsync();
                client.Connect("127.0.0.1", ObterPorta(listener));
                using (TcpClient servidor = await Aguardar(aceita))
                {
                    Task<Message> resposta = client.WriteLineAndGetReplyAsync("pedido", TimeSpan.FromSeconds(2));
                    Assert.AreEqual("pedido", await LerLinhaAsync(servidor));
                    Escrever(servidor, "res");
                    Escrever(servidor, "posta\x13");
                    Assert.AreEqual("resposta", (await Aguardar(resposta)).MessageString);
                }
            }
        }

        [TestMethod]
        public async Task Requisicoes_concorrentes_sao_serializadas_e_recebem_respostas_em_ordem()
        {
            TcpListener listener = CriarListener();
            using (SimpleTcpClient client = new SimpleTcpClient())
            {
                Task<TcpClient> aceita = listener.AcceptTcpClientAsync();
                client.Connect("127.0.0.1", ObterPorta(listener));
                using (TcpClient servidor = await Aguardar(aceita))
                {
                    Task<Message> primeira = client.WriteLineAndGetReplyAsync("primeira", TimeSpan.FromSeconds(2));
                    Task<Message> segunda = client.WriteLineAndGetReplyAsync("segunda", TimeSpan.FromSeconds(2));
                    Assert.AreEqual("primeira", await LerLinhaAsync(servidor));
                    Escrever(servidor, "resposta-1\x13");
                    Assert.AreEqual("segunda", await LerLinhaAsync(servidor));
                    Escrever(servidor, "resposta-2\x13");

                    Assert.AreEqual("resposta-1", (await Aguardar(primeira)).MessageString);
                    Assert.AreEqual("resposta-2", (await Aguardar(segunda)).MessageString);
                }
            }
        }

        [TestMethod]
        public async Task Timeout_antes_do_semaforo_nao_envia_requisicao()
        {
            TcpListener listener = CriarListener();
            using (SimpleTcpClient client = new SimpleTcpClient())
            {
                Task<TcpClient> aceita = listener.AcceptTcpClientAsync();
                client.Connect("127.0.0.1", ObterPorta(listener));
                using (TcpClient servidor = await Aguardar(aceita))
                {
                    Task<Message> primeira = client.WriteLineAndGetReplyAsync("primeira", TimeSpan.FromMilliseconds(250));
                    Assert.AreEqual("primeira", await LerLinhaAsync(servidor));
                    Message segunda = await client.WriteLineAndGetReplyAsync("segunda", TimeSpan.FromMilliseconds(30));
                    Assert.IsNull(segunda);
                    Assert.IsFalse(await HaDadosAsync(servidor, TimeSpan.FromMilliseconds(100)));
                    Assert.IsNull(await Aguardar(primeira));
                }
            }
        }

        [TestMethod]
        public async Task Timeout_apos_envio_fecha_sessao_e_resposta_tardia_nao_contamina_nova_sessao()
        {
            TcpListener listener = CriarListener();
            using (SimpleTcpClient client = new SimpleTcpClient())
            {
                Task<TcpClient> primeiraAceita = listener.AcceptTcpClientAsync();
                client.Connect("127.0.0.1", ObterPorta(listener));
                using (TcpClient primeiroServidor = await Aguardar(primeiraAceita))
                {
                    Task<Message> respostaAntiga = client.WriteLineAndGetReplyAsync("antiga", TimeSpan.FromMilliseconds(100));
                    Assert.AreEqual("antiga", await LerLinhaAsync(primeiroServidor));
                    Assert.IsNull(await Aguardar(respostaAntiga));
                    Assert.AreEqual(0, await Aguardar(primeiroServidor.GetStream().ReadAsync(new byte[1], 0, 1)));
                    Escrever(primeiroServidor, "tardia\x13");
                }

                Task<TcpClient> segundaAceita = listener.AcceptTcpClientAsync();
                client.Connect("127.0.0.1", ObterPorta(listener));
                using (TcpClient segundoServidor = await Aguardar(segundaAceita))
                {
                    Task<Message> respostaNova = client.WriteLineAndGetReplyAsync("nova", TimeSpan.FromSeconds(2));
                    Assert.AreEqual("nova", await LerLinhaAsync(segundoServidor));
                    Escrever(segundoServidor, "correta\x13");
                    Assert.AreEqual("correta", (await Aguardar(respostaNova)).MessageString);
                }
            }
        }

        [TestMethod]
        public async Task Cancelamento_apos_envio_fecha_sessao_e_lanca_excecao()
        {
            TcpListener listener = CriarListener();
            using (SimpleTcpClient client = new SimpleTcpClient())
            using (CancellationTokenSource cancelamento = new CancellationTokenSource())
            {
                Task<TcpClient> aceita = listener.AcceptTcpClientAsync();
                client.Connect("127.0.0.1", ObterPorta(listener));
                using (TcpClient servidor = await Aguardar(aceita))
                {
                    Task<Message> resposta = client.WriteLineAndGetReplyAsync("cancelar", TimeSpan.FromSeconds(2), cancelamento.Token);
                    Assert.AreEqual("cancelar", await LerLinhaAsync(servidor));
                    cancelamento.Cancel();
                    await Assert.ThrowsExactlyAsync<OperationCanceledException>(async () => await resposta);
                    Assert.AreEqual(0, await Aguardar(servidor.GetStream().ReadAsync(new byte[1], 0, 1)));
                }
            }
        }

        [TestMethod]
        public async Task Desconexao_remota_durante_espera_retorna_nulo()
        {
            TcpListener listener = CriarListener();
            using (SimpleTcpClient client = new SimpleTcpClient())
            {
                Task<TcpClient> aceita = listener.AcceptTcpClientAsync();
                client.Connect("127.0.0.1", ObterPorta(listener));
                TcpClient servidor = await Aguardar(aceita);
                Task<Message> resposta = client.WriteLineAndGetReplyAsync("pedido", TimeSpan.FromSeconds(2));
                Assert.AreEqual("pedido", await LerLinhaAsync(servidor));
                servidor.Close();
                Assert.IsNull(await Aguardar(resposta));
            }
        }

        [TestMethod]
        public async Task Bytes_adicionam_um_unico_delimitador()
        {
            TcpListener listener = CriarListener();
            using (SimpleTcpClient client = new SimpleTcpClient())
            {
                Task<TcpClient> aceita = listener.AcceptTcpClientAsync();
                client.Connect("127.0.0.1", ObterPorta(listener));
                using (TcpClient servidor = await Aguardar(aceita))
                {
                    Task<Message> resposta = client.WriteLineAndGetReplyAsync(new byte[] { 1, 2, 0x13 }, TimeSpan.FromSeconds(2));
                    byte[] linha = await LerLinhaBytesAsync(servidor);
                    CollectionAssert.AreEqual(new byte[] { 1, 2 }, linha);
                    Escrever(servidor, "ok\x13");
                    Assert.AreEqual("ok", (await Aguardar(resposta)).MessageString);
                }
            }
        }

        [TestMethod]
        public async Task Assinante_com_excecao_nao_bloqueia_resposta()
        {
            TcpListener listener = CriarListener();
            using (SimpleTcpClient client = new SimpleTcpClient())
            {
                client.DelimiterDataReceived += (sender, mensagem) => { throw new InvalidOperationException("Falha esperada no assinante."); };
                Task<TcpClient> aceita = listener.AcceptTcpClientAsync();
                client.Connect("127.0.0.1", ObterPorta(listener));
                using (TcpClient servidor = await Aguardar(aceita))
                {
                    Task<Message> resposta = client.WriteLineAndGetReplyAsync("pedido", TimeSpan.FromSeconds(2));
                    Assert.AreEqual("pedido", await LerLinhaAsync(servidor));
                    Escrever(servidor, "ok\x13");
                    Assert.AreEqual("ok", (await Aguardar(resposta)).MessageString);
                }
            }
        }

        [TestMethod]
        public async Task Reconnect_nao_reenvia_requisicao_ambigua()
        {
            TcpListener listener = CriarListener();
            using (SimpleTcpClient client = new SimpleTcpClient())
            {
                Task<TcpClient> primeiraAceita = listener.AcceptTcpClientAsync();
                client.Connect("127.0.0.1", ObterPorta(listener));
                using (TcpClient primeiroServidor = await Aguardar(primeiraAceita))
                {
                    Task<SimpleTcpClient> reconexao = Task.Run(() => client.ReconnectWriteLineAndGetReply("nao-reenviar", TimeSpan.FromMilliseconds(100)));
                    Assert.AreEqual("nao-reenviar", await LerLinhaAsync(primeiroServidor));
                    Task<TcpClient> segundaAceita = listener.AcceptTcpClientAsync();
                    using (TcpClient segundoServidor = await Aguardar(segundaAceita))
                    {
                        Assert.AreSame(client, await Aguardar(reconexao));
                        Assert.IsFalse(await HaDadosAsync(segundoServidor, TimeSpan.FromMilliseconds(100)));
                    }
                }
            }
        }

        [TestMethod]
        public void Timeout_acima_do_limite_do_net462_e_rejeitado_explicitamente()
        {
            using (SimpleTcpClient client = new SimpleTcpClient())
            {
                TimeSpan timeoutInvalido = TimeSpan.FromMilliseconds((double)int.MaxValue + 1);
                ArgumentOutOfRangeException excecao = Assert.ThrowsExactly<ArgumentOutOfRangeException>(
                    () => client.WriteLineAndGetReplyAsync("pedido", timeoutInvalido).GetAwaiter().GetResult());

                StringAssert.Contains(excecao.Message, "Int32.MaxValue");
            }
        }

        private TcpListener CriarListener()
        {
            TcpListener listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            _listeners.Add(listener);
            return listener;
        }

        private static int ObterPorta(TcpListener listener)
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }

        private static void Escrever(TcpClient client, string texto)
        {
            byte[] dados = Encoding.UTF8.GetBytes(texto);
            client.GetStream().Write(dados, 0, dados.Length);
        }

        private static async Task<string> LerLinhaAsync(TcpClient client)
        {
            return Encoding.UTF8.GetString(await LerLinhaBytesAsync(client));
        }

        private static async Task<byte[]> LerLinhaBytesAsync(TcpClient client)
        {
            List<byte> dados = new List<byte>();
            byte[] buffer = new byte[256];
            while (true)
            {
                int lidos = await client.GetStream().ReadAsync(buffer, 0, buffer.Length);
                Assert.IsTrue(lidos > 0, "A conexão foi encerrada antes do delimitador.");
                for (int indice = 0; indice < lidos; indice++)
                {
                    if (buffer[indice] == 0x13)
                    {
                        return dados.ToArray();
                    }

                    dados.Add(buffer[indice]);
                }
            }
        }

        private static async Task<bool> HaDadosAsync(TcpClient client, TimeSpan timeout)
        {
            Task<int> leitura = client.GetStream().ReadAsync(new byte[1], 0, 1);
            Task concluida = await Task.WhenAny(leitura, Task.Delay(timeout));
            return ReferenceEquals(concluida, leitura) && await leitura > 0;
        }

        private static async Task<T> Aguardar<T>(Task<T> task)
        {
            Task concluida = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(5)));
            Assert.AreSame(task, concluida, "A operação TCP não foi concluída dentro do tempo esperado.");
            return await task;
        }
    }
}
