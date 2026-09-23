using System;
using System.Linq;
using System.Net;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace FactorySI.SimpleTcp.Tests
{
	[TestClass]
	public class ServerTests2
	{
		[TestMethod]
		public void Start_passes_if_at_all_nics_passed()
		{
			var server = new SimpleTcpServer().Start(ObterPortaDisponivel(), false);
			try
			{
				Assert.IsTrue(server.IsStarted, "O servidor deveria ter sido iniciado.");
			}
			finally
			{
				server.Stop();
			}
		}

		[TestMethod]
		public void Start_passes_if_at_least_one_nic_is_free()
		{
			var porta = ObterPortaDisponivel();
			var listener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, porta);
			listener.Start();
			SimpleTcpServer server = null;
			try
			{
				server = new SimpleTcpServer().Start(porta);
				Assert.IsTrue(server.IsStarted, "O servidor deveria ter iniciado nas interfaces livres.");
			}
			finally
			{
				if (server != null)
				{
					server.Stop();
				}

				listener.Stop();
			}
		}

		[TestMethod]
		public void Start_fails_if_at_all_nics_free_is_required()
		{
			var porta = ObterPortaDisponivel();
			var listener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, porta);
			listener.Start();
			try
			{
				Assert.ThrowsExactly<InvalidOperationException>(() =>
				{
					SimpleTcpServer server = null;
					try
					{
						server = new SimpleTcpServer().Start(porta, false);
					}
					finally
					{
						if (server != null)
						{
							server.Stop();
						}
					}
				});
			}
			finally
			{
				listener.Stop();
			}
		}

		private static int ObterPortaDisponivel()
		{
			var listener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
			listener.Start();
			var porta = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
			listener.Stop();
			return porta;
		}
	}
}
