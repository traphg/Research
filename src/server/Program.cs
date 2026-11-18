using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

class Program
{
    static async Task Main(string[] args)
    {
        string ip = args.Length > 0 ? args[0] : "127.0.0.1";
        int port = args.Length > 1 ? int.Parse(args[1]) : 9000;

        string outDir = "experiments/raw-data";
        Directory.CreateDirectory(outDir);
        File.WriteAllText(Path.Combine(outDir, "server.pid"), Environment.ProcessId.ToString());

        // Chạy luồng nền ghi log CPU theo giây (dữ liệu đối chiếu phụ)
        _ = MonitorServerCpuAsync(outDir);

        var listener = new TcpListener(IPAddress.Parse(ip), port);
        listener.Start();
        Console.WriteLine($"[Server] Listening on {ip}:{port} (PID: {Environment.ProcessId})");

        while (true)
        {
            TcpClient client = await listener.AcceptTcpClientAsync();
            _ = HandleClientAsync(client);
        }
    }

    private static async Task HandleClientAsync(TcpClient client)
    {
        using (client)
        {
            client.NoDelay = true;
            var stream = client.GetStream();
            byte[] lenBuf = new byte[Protocol.LengthFieldSize];

            try
            {
                while (true)
                {
                    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

                    // 1. Đọc 4 bytes BodyLength
                    bool lenOk = await Protocol.ReadExactAsync(
                        stream, lenBuf, 0, Protocol.LengthFieldSize, cts.Token);
                    if (!lenOk) break; // Client đóng kết nối bìnhnh thường

                    int bodyLength = BinaryPrimitives.ReadInt32BigEndian(lenBuf.AsSpan(0, 4));

                    // 2. Kiểm tra giới hạn BodyLength (4..10244 bytes)
                    if (bodyLength < Protocol.RequestIdSize || bodyLength > Protocol.MaxBodyLength)
                    {
                        Console.WriteLine(
                            $"[Server] Từ chối gói tin không hợp lệ (BodyLength={bodyLength} vượt giới hạn {Protocol.MaxBodyLength}).");
                        break;
                    }

                    int totalFrameLength = Protocol.LengthFieldSize + bodyLength;
                    byte[] frame = new byte[totalFrameLength];
                    Buffer.BlockCopy(lenBuf, 0, frame, 0, Protocol.LengthFieldSize);

                    // 3. Đọc phần Body (RequestId + Payload)
                    bool bodyOk = await Protocol.ReadExactAsync(
                        stream, frame, Protocol.LengthFieldSize, bodyLength, cts.Token);
                    if (!bodyOk)
                    {
                        Console.WriteLine(
                            "[Server] Client đóng kết nối bất thường khi đang gửi payload.");
                        break;
                    }

                    // 4. Echo trả lại đúng frame cho Client
                    await stream.WriteAsync(frame.AsMemory(0, totalFrameLength), cts.Token);
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("[Server] Client timeout quá 5 giây.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Server] Lỗi kết nối Client: {ex.Message}");
            }
        }
    }

    private static async Task MonitorServerCpuAsync(string outDir)
    {
        var proc = Process.GetCurrentProcess();
        string cpuLogPath = Path.Combine(outDir, "server_cpu.csv");

        if (!File.Exists(cpuLogPath))
            File.WriteAllText(cpuLogPath, "TimestampUtc,ServerPid,CpuPercent\n");

        TimeSpan lastCpuTime = proc.TotalProcessorTime;
        var sw = Stopwatch.StartNew();

        while (true)
        {
            await Task.Delay(1000);
            proc.Refresh();
            TimeSpan currentCpuTime = proc.TotalProcessorTime;
            double elapsedMs = sw.Elapsed.TotalMilliseconds;
            sw.Restart();

            double cpuUsedMs = (currentCpuTime - lastCpuTime).TotalMilliseconds;
            lastCpuTime = currentCpuTime;

            double cpuPercent = (cpuUsedMs / (elapsedMs * Environment.ProcessorCount)) * 100.0;
            File.AppendAllText(cpuLogPath,
                string.Format(CultureInfo.InvariantCulture, "{0:o},{1},{2:F2}\n",
                    DateTime.UtcNow, proc.Id, cpuPercent));
        }
    }
}
