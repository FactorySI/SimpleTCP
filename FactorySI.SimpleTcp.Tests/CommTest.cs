using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace FactorySI.SimpleTcp.Tests
{
    [TestClass]
    public class CommTest
    {
        [TestMethod]
        public void SimpleCommTest()
        {
            var mensagensClienteEnviadas = new List<string>();
            var mensagensClienteRecebidas = new List<string>();
            var mensagensServidorRecebidas = new List<string>();
            var mensagensServidorEnviadas = new List<string>();
            var mensagensServidorRecebidasEvent = new ManualResetEvent(false);
            var mensagensClienteRecebidasEvent = new ManualResetEvent(false);
            var clienteConectadoEvent = new ManualResetEvent(false);
            var porta = ObterPortaDisponivel();
            var servidor = new SimpleTcpServer().Start(IPAddress.Loopback, porta);
            SimpleTcpClient cliente = null;

            try
            {
                servidor.ClientConnected += (sender, tcpClient) => clienteConectadoEvent.Set();
                servidor.DelimiterDataReceived += (sender, mensagem) =>
                {
                    lock (mensagensServidorRecebidas)
                    {
                        mensagensServidorRecebidas.Add(mensagem.MessageString);
                        mensagensServidorEnviadas.Add(mensagem.MessageString);

                        if (mensagensServidorRecebidas.Count == 10)
                        {
                            mensagensServidorRecebidasEvent.Set();
                        }
                    }

                    mensagem.ReplyLine(mensagem.MessageString);
                };

                cliente = new SimpleTcpClient(new SimpleTcpParam { Name = "Teste" }).Connect(IPAddress.Loopback.ToString(), porta);
                cliente.DelimiterDataReceived += (sender, mensagem) =>
                {
                    lock (mensagensClienteRecebidas)
                    {
                        mensagensClienteRecebidas.Add(mensagem.MessageString);
                        if (mensagensClienteRecebidas.Count == 10)
                        {
                            mensagensClienteRecebidasEvent.Set();
                        }
                    }
                };

                Assert.IsTrue(clienteConectadoEvent.WaitOne(TimeSpan.FromSeconds(5)), "O servidor não registrou a conexão do cliente.");

                for (var indice = 0; indice < 10; indice++)
                {
                    var mensagem = Guid.NewGuid().ToString();
                    mensagensClienteEnviadas.Add(mensagem);
                    cliente.WriteLine(mensagem);
                }

                Assert.IsTrue(mensagensServidorRecebidasEvent.WaitOne(TimeSpan.FromSeconds(5)), "O servidor não recebeu todas as mensagens do cliente.");
                Assert.IsTrue(mensagensClienteRecebidasEvent.WaitOne(TimeSpan.FromSeconds(5)), "O cliente não recebeu todas as respostas do servidor.");

                CollectionAssert.AreEqual(mensagensClienteEnviadas, mensagensServidorRecebidas);
                CollectionAssert.AreEqual(mensagensServidorEnviadas, mensagensClienteRecebidas);

                var resposta = cliente.WriteLineAndGetReply("TESTE", TimeSpan.FromSeconds(1));
                Assert.IsNotNull(resposta, "WriteLineAndGetReply não deveria retornar nulo.");
            }
            finally
            {
                if (cliente != null)
                {
                    cliente.Dispose();
                }

                servidor.Stop();
                mensagensServidorRecebidasEvent.Close();
                mensagensClienteRecebidasEvent.Close();
                clienteConectadoEvent.Close();
            }
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
