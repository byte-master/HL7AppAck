using System;
using System.Configuration;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using Azure.Storage.Queues; 
using QueueMessage = Azure.Storage.Queues.Models.QueueMessage;







Console.WriteLine("Ap statred");
string QueueName = ConfigurationManager.AppSettings["QueueName"];
ProcessMessages(QueueName);
Console.WriteLine("Ap ended");

void log(string txt)
{
    Console.WriteLine(DateTime.Now.ToString() + ": " + txt);
/*    StreamWriter sw;
    string path = "LogOutput.txt";
    // This text is added only once to the file.
    if (!File.Exists(path))
    {
        // Create a file to write to.
        sw = File.CreateText(path);
    }
    else
    {
        sw = File.AppendText(path);
    }

    sw.WriteLine(DateTime.Now.ToString()+": "+ txt);
    sw.Close();*/
}

    void ProcessMessages(string queueName)
{
    
    // Get the connection string from app settings
    string connectionString = ConfigurationManager.AppSettings["StorageConnectionString"];

    // Instantiate a QueueClient which will be used to manipulate the queue
    QueueClient queueClient = new QueueClient(connectionString, queueName);
    log("Connected to storage");
    if (queueClient.Exists())
    {
        log("Queue Exsists");
        QueueMessage[] retrievedMessage = queueClient.ReceiveMessages();
        while (retrievedMessage.Length>0)
        {
            foreach (QueueMessage m in retrievedMessage)
            {
                bool ret = false;
                //var m = queueClient.ReceiveMessage();
                string tmp = m.Body.ToString();
                var t = Convert.FromBase64String(tmp);
                string str = Encoding.Default.GetString(t);
                log("Retrieved Message: "+str);
                

                if (AcknowledgeMessage(str))
                {
                    log("Message AppAck sent");
                    queueClient.DeleteMessage(retrievedMessage[0].MessageId, retrievedMessage[0].PopReceipt); Console.WriteLine(str);
                    log(str + " removed from queue");
                }
                else
                {
                    log("Message AppAck failed to send");
                    Console.WriteLine(str + " Send failed");
                }
                

            }
            retrievedMessage = queueClient.ReceiveMessages();
        }
    }

    bool SendAppAck(string message)
    {
        string url = ConfigurationManager.AppSettings["MPIEndPoint"];
        string port = ConfigurationManager.AppSettings["MPIPort"];
        int returncode = 0;
        bool ret = false;
        try
        {
            using (var client = new HttpClient())
            {
                client.BaseAddress = new Uri(url + ":" + port);
                var content = new FormUrlEncodedContent(new[]
                {
                    new KeyValuePair<string, string>("data", message)
                });
                var result = client.PostAsync("", content).Result;
                returncode = ((int)result.StatusCode);
            }
            ret = (returncode == 200);
        }
        catch { }
        return ret;
    }
    bool AcknowledgeMessage(string message)
    {
        // Application Ack message needs to go through a different tcp connection
        TcpClient outTcpClient = null;
        NetworkStream outStream = null;
        bool ret = false;
        int port =Convert.ToInt32(ConfigurationManager.AppSettings["MpiAckTcpPort"]);
        try
        {
            var appAckMessage = message;
            log("Begin App Ack" + message);
            if (!string.IsNullOrEmpty(appAckMessage))
            {
                var url = ConfigurationManager.AppSettings["MpiAckTcpAddress"];
                IPAddress ipaddress = IPAddress.Parse(url);
                appAckMessage = $"{(char)0x0b}{appAckMessage}{(char)0x1C}{(char)0x0d}";
                var byteAppAckMessage = appAckMessage.ToArray<char>;
                byte[] byteArray = Encoding.ASCII.GetBytes(appAckMessage);

                outTcpClient = new TcpClient();
                log("Sending ACK to: " + url + ":" + port.ToString());
                outTcpClient.Connect(new IPEndPoint(ipaddress, port));
                outStream = outTcpClient.GetStream();
                outStream.Write(byteArray, 0, appAckMessage.Length);
                log("Sent AA Ack");

                ret = true;
            }
        }
        catch (Exception ex)
        {
            log(ex.Message);

        }
        finally
        {
            outStream?.Close();
            outTcpClient?.Close();
        }
        return ret;
    }

}
