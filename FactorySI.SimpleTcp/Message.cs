using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace FactorySI.SimpleTcp
{
    public class Message
    {
        private TcpClient _tcpClient;
        private System.Text.Encoding _encoder = null;
        private byte _writeLineDelimiter;
        private bool _autoTrim = false;
        private Action<byte[]> _write = null;
        internal Message(byte[] data, TcpClient tcpClient, System.Text.Encoding stringEncoder, byte lineDelimiter)
        {
            Data = data;
            _tcpClient = tcpClient;
            _encoder = stringEncoder;
            _writeLineDelimiter = lineDelimiter;
        }

        internal Message(byte[] data, TcpClient tcpClient, System.Text.Encoding stringEncoder, byte lineDelimiter, bool autoTrim)
            : this(data, tcpClient, stringEncoder, lineDelimiter, autoTrim, null)
        {
        }

        internal Message(byte[] data, TcpClient tcpClient, System.Text.Encoding stringEncoder, byte lineDelimiter, bool autoTrim, Action<byte[]> write)
        {
            Data = data;
            _tcpClient = tcpClient;
            _encoder = stringEncoder;
            _writeLineDelimiter = lineDelimiter;
            _autoTrim = autoTrim;
            _write = write;
        }

        public byte[] Data { get; private set; }
        public string MessageString
        {
            get
            {
                if (_autoTrim)
                {
                    return _encoder.GetString(Data).Trim();
                }

                return _encoder.GetString(Data);
            }
        }

        public void Reply(byte[] data)
        {
            if (_write != null)
            {
                _write(data);
                return;
            }

            _tcpClient.GetStream().Write(data, 0, data.Length);
        }

        public void Reply(string data)
        {
            if (string.IsNullOrEmpty(data)) { return; }
            Reply(_encoder.GetBytes(data));
        }

        public void ReplyLine(string data)
        {
            if (string.IsNullOrEmpty(data)) { return; }
            if (data.LastOrDefault() != _writeLineDelimiter)
            {
                Reply(data + _encoder.GetString(new byte[] { _writeLineDelimiter }));
            } else
            {
                Reply(data);
            }
        }

        public TcpClient TcpClient {  get { return _tcpClient; } }
    }
}
