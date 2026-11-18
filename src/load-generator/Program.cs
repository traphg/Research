using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

class Program
{
    static async Task<int> Main(string[] args)
    {
        try
        {
            // Chế độ kiểm thử lỗi cho TV3 (QA): KHÔNG ghi vào CSV benchmark
            if (args.Length > 0 && args[0] == "--fault-test")
            {
                await RunFaultTestsAsync("127.0.0.1", 9000);
                return 0;
            }

            string configPath = args.Length > 0 ? args[0] : "experiments/configs/load_10.json";
            var cfg = LoadConfig.FromFile(configPath);

            string runId = args.Length > 1 ? args[1] : $"c{cfg.ClientCount:D2}-r1";
            int repeat = args.Length > 2 && int.TryParse(args[2], out int r) ? r : 1;
            if (repeat <= 0) throw new ArgumentException("Số thứ tự lần lặp (Repeat) phải >= 1.");

            int serverPid = ResolveServerPid(args);
            using Process serverProc = GetValidServerProcess(serverPid);

            Console.WriteLine($"[LoadGen] RunId={runId} | Repeat={repeat} | Clients={cfg.ClientCount} | " +
                $"Warmup={cfg.WarmupSec}s | Measure={cfg.DurationSec}s | Payload={cfg.PayloadBytes}B | ServerPID={serverPid}");

            var workers = new ClientWorker[cfg.ClientCount];
            for (int i = 0; i < cfg.ClientCount; i++)
                workers[i] = new ClientWorker(cfg, i + 1);

            try
            {
                // BƯỚC 1: Mở đủ kết nối TCP cho toàn bộ Client trước khi tính giờ Warm-up
                await Task.WhenAll(workers.Select(w => w.ConnectAsync()));
                Console.WriteLine($"[LoadGen] Đã kết nối thành công {cfg.ClientCount}/{cfg.ClientCount} Clients. Bắt đầu Warm-up {cfg.WarmupSec}s...");

                // BƯỚC 2: Đồng loạt chạy Warm-up đến đúng mốc warmupDeadlineTick
                var warmupStartGate = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
                var warmupTasks = workers.Select(w => w.RunWarmupAsync(warmupStartGate.Task)).ToArray();

                long warmupStartTick = Stopwatch.GetTimestamp();
                long warmupDeadlineTick = warmupStartTick + (long)(cfg.WarmupSec * (double)Stopwatch.Frequency);
                warmupStartGate.TrySetResult(warmupDeadlineTick);
                await Task.WhenAll(warmupTasks);

                Console.WriteLine($"[LoadGen] Hoàn tất Warm-up. Bắt đầu cửa sổ đo chính thức {cfg.DurationSec}s...");

                // BƯỚC 3: Đồng loạt mở cửa sổ đo chính thức đúng 20 giây với measureDeadlineTick
                var measureStartGate = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
                var measureTasks = workers.Select(w => w.RunMeasureAsync(measureStartGate.Task)).ToArray();

                serverProc.Refresh();
                if (serverProc.HasExited)
                    throw new InvalidOperationException("Tiến trình TcpServer đã dừng trước khi đo!");

                TimeSpan cpuStart = serverProc.TotalProcessorTime;
                var measureSw = Stopwatch.StartNew();
                long measureStartTick = Stopwatch.GetTimestamp();
                long measureDeadlineTick = measureStartTick + (long)(cfg.DurationSec * (double)Stopwatch.Frequency);

                // Mở cổng truyền đúng measureDeadlineTick cho toàn bộ Client
                measureStartGate.TrySetResult(measureDeadlineTick);

                // Đợi đến đúng mốc hết cửa sổ khởi tạo request (tránh busy-wait Task.Yield làm tốn CPU)
                long ticksRemaining = measureDeadlineTick - Stopwatch.GetTimestamp();
                if (ticksRemaining > 0)
                {
                    int msRemaining = (int)Math.Ceiling(ticksRemaining * 1000.0 / Stopwatch.Frequency);
                    if (msRemaining > 0)
                    {
                        await Task.Delay(msRemaining);
                    }
                }

                measureSw.Stop();

                // Chụp ngay thời lượng CPU sampling thực tế và CPU của Server
                serverProc.Refresh();
                if (serverProc.HasExited)
                    throw new InvalidOperationException("Tiến trình TcpServer đã dừng trong khi đo!");
                TimeSpan cpuEnd = serverProc.TotalProcessorTime;

                // Chờ các request cuối cùng đã được gửi trước measureDeadlineTick hoàn tất nhận response (drain)
                await Task.WhenAll(measureTasks);

                // BƯỚC 4: Tính toán 5 chỉ số hiệu năng
                // Cửa sổ khởi tạo request danh định theo đúng 2 mốc tick (chuẩn 20.0s)
                double actualMeasureSec = (measureDeadlineTick - measureStartTick) / (double)Stopwatch.Frequency;

                // Mức sử dụng CPU của Server đo trên khoảng thời gian CPU sampling thực tế
                double cpuElapsedMs = Math.Max(1.0, measureSw.Elapsed.TotalMilliseconds);
                double cpuUsedMs = (cpuEnd - cpuStart).TotalMilliseconds;
                double serverCpuPercent = (cpuUsedMs / (cpuElapsedMs * Environment.ProcessorCount)) * 100.0;

                var allLatencies = new List<double>();
                long totalSuccess = 0, totalErrors = 0;
                foreach (var w in workers)
                {
                    allLatencies.AddRange(w.LatenciesMs);
                    totalSuccess += w.SuccessCount;
                    totalErrors += w.ErrorCount;
                }

                long totalRequests = totalSuccess + totalErrors;
                if (totalRequests == 0)
                    throw new InvalidOperationException("Không có request nào được thực hiện trong giai đoạn đo!");

                double avgMs = allLatencies.Count > 0 ? allLatencies.Average() : 0;
                double p95Ms = allLatencies.Count > 0 ? NearestRankPercentile(allLatencies, 0.95) : 0;
                double errRatePercent = (totalErrors * 100.0) / totalRequests;
                double throughputRps = totalSuccess / actualMeasureSec;

                Console.WriteLine(
                    $"[Result] RunId={runId} | Window={actualMeasureSec:F3}s | Total={totalRequests}, Success={totalSuccess}, Errors={totalErrors} | " +
                    $"Avg={avgMs:F3}ms, P95={p95Ms:F3}ms | ErrRate={errRatePercent:F4}% | " +
                    $"Throughput={throughputRps:F2} req/s | ServerCPU={serverCpuPercent:F2}%");

                // BƯỚC 5: Ghi file run_results.csv (Đầy đủ 16 cột)
                string outDir = "experiments/raw-data";
                Directory.CreateDirectory(outDir);
                string resultsCsvPath = Path.Combine(outDir, "run_results.csv");
                if (!File.Exists(resultsCsvPath))
                {
                    File.WriteAllText(resultsCsvPath,
                        "RunId,TimestampUtc,ClientCount,Repeat,PayloadBytes,WarmupSeconds,MeasureSeconds,ActualMeasureSeconds," +
                        "TotalRequests,SuccessRequests,ErrorRequests,AvgLatencyMs,P95LatencyMs,ThroughputRps,ErrorRatePercent,ServerCpuPercent\n");
                }

                string resultLine = string.Format(CultureInfo.InvariantCulture,
                    "{0},{1:o},{2},{3},{4},{5},{6},{7:F4},{8},{9},{10},{11:F4},{12:F4},{13:F2},{14:F4},{15:F2}\n",
                    runId, DateTime.UtcNow, cfg.ClientCount, repeat, cfg.PayloadBytes, cfg.WarmupSec, cfg.DurationSec, actualMeasureSec,
                    totalRequests, totalSuccess, totalErrors, avgMs, p95Ms, throughputRps, errRatePercent, serverCpuPercent);
                File.AppendAllText(resultsCsvPath, resultLine);

                // BƯỚC 6: Ghi toàn bộ 100% request đo được vào latency_samples.csv bằng StreamWriter
                string samplesCsvPath = Path.Combine(outDir, "latency_samples.csv");
                bool writeHeader = !File.Exists(samplesCsvPath);
                using (var writer = new StreamWriter(samplesCsvPath, append: true, Encoding.UTF8, bufferSize: 65536))
                {
                    if (writeHeader)
                    {
                        await writer.WriteLineAsync("RunId,ClientId,RequestId,LatencyMs,Status");
                    }
                    foreach (var w in workers)
                    {
                        foreach (var s in w.Samples)
                        {
                            await writer.WriteLineAsync(string.Format(CultureInfo.InvariantCulture,
                                "{0},{1},{2},{3:F4},{4}",
                                runId, s.ClientId, s.RequestId, s.LatencyMs, s.Status));
                        }
                    }
                }

                Console.WriteLine($"[CSV] Đã lưu kết quả (16 cột) vào {resultsCsvPath} và toàn bộ {totalRequests} mẫu request vào {samplesCsvPath}");
                return 0;
            }
            finally
            {
                foreach (var w in workers) w?.Dispose();
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[LỖI LẦN CHẠY KHÔNG HỢP LỆ] {ex.Message}");
            return 1;
        }
    }

    static int ResolveServerPid(string[] args)
    {
        if (args.Length > 3)
        {
            if (!int.TryParse(args[3], out int pid) || pid <= 0)
                throw new ArgumentException($"Server PID truyền vào không hợp lệ: {args[3]}");
            return pid;
        }

        string pidFile = Path.Combine("experiments", "raw-data", "server.pid");
        if (File.Exists(pidFile) && int.TryParse(File.ReadAllText(pidFile).Trim(), out int savedPid) && savedPid > 0)
        {
            return savedPid;
        }

        throw new ArgumentException(
            "Không tìm thấy Server PID! Hãy truyền PID của TcpServer vào tham số thứ 4 (ví dụ: dotnet run ... load_1.json c01-r1 1 <SERVER_PID>).");
    }

    static Process GetValidServerProcess(int serverPid)
    {
        try
        {
            Process proc = Process.GetProcessById(serverPid);
            if (proc.HasExited)
                throw new InvalidOperationException($"Tiến trình Server (PID={serverPid}) đã kết thúc.");
            return proc;
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"Không thể kết nối tới tiến trình TcpServer với PID={serverPid} để đo CPU. Hãy kiểm tra lại PID trên cửa sổ Server. Chi tiết: {ex.Message}");
        }
    }

    static double NearestRankPercentile(List<double> seq, double pct)
    {
        seq.Sort();
        int idx = (int)Math.Ceiling(pct * seq.Count) - 1;
        return seq[Math.Max(0, idx)];
    }

    // BỘ KIỂM THỬ LỖI CHO TV3 (QA TESTER) - KHÔNG GHI VÀO CSV BENCHMARK
    static async Task RunFaultTestsAsync(string host, int port)
    {
        Console.WriteLine("=== CHẠY KIỂM THỬ LỖI (FAULT TESTS - MỤC 4) ===\n");

        // Test 1: Gửi BodyLength vượt giới hạn (50 KB > MaxBodyLength 10244 bytes)
        Console.Write("[Test 1] Gửi gói tin vượt giới hạn 50 KB...");
        using (var c1 = new TcpClient())
        {
            using var cts1 = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await c1.ConnectAsync(host, port, cts1.Token);
            byte[] badHeader = new byte[8];
            BinaryPrimitives.WriteInt32BigEndian(badHeader.AsSpan(0, 4), 50 * 1024);
            await c1.GetStream().WriteAsync(badHeader, cts1.Token);
        }
        await Task.Delay(500);
        Console.WriteLine(" ✔ Đã gửi và đóng kết nối. Kiểm tra log Server -> 'Từ chối gói tin không hợp lệ'.");

        // Test 2A: Client ngắt kết nối đột ngột giữa chừng (Đóng kết nối và chờ 500ms để Server xuất log trước khi in thông báo)
        Console.Write("[Test 2A] Ngắt kết nối đột ngột khi đang gửi dở payload...");
        using (var c2 = new TcpClient())
        {
            using var cts2 = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await c2.ConnectAsync(host, port, cts2.Token);
            byte[] partialFrame = new byte[16];
            BinaryPrimitives.WriteInt32BigEndian(partialFrame.AsSpan(0, 4), 1028);
            await c2.GetStream().WriteAsync(partialFrame, cts2.Token);
            c2.Close();
        }
        await Task.Delay(500);
        Console.WriteLine(" ✔ Đã ngắt kết nối. Kiểm tra log Server -> 'Client đóng kết nối bất thường'.");

        // Test 2B: Client treo kết nối quá 5 giây (Kiểm tra Timeout)
        Console.Write("[Test 2B] Giữ kết nối treo 5.5 giây để test Timeout...");
        using (var cTimeout = new TcpClient())
        {
            using var ctsTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await cTimeout.ConnectAsync(host, port, ctsTimeout.Token);
            await Task.Delay(5500);
        }
        Console.WriteLine(" ✔ Đã chờ 5.5s. Kiểm tra log Server -> 'Client timeout quá 5 giây'.");

        // Test 3: Kiểm tra Server vẫn hoạt động bình thường sau lỗi (Có CancellationToken 5 giây chống treo)
        Console.Write("[Test 3] Kiểm tra Server vẫn sống và phản hồi khớp 100% RequestId + Payload...");
        try
        {
            using var cts3 = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            using var c3 = new TcpClient();
            await c3.ConnectAsync(host, port, cts3.Token);
            byte[] payload = new byte[1024];
            for (int i = 0; i < payload.Length; i++) payload[i] = (byte)(i % 256);
            byte[] msg = Protocol.Encode(999, payload);
            await c3.GetStream().WriteAsync(msg, cts3.Token);

            byte[] recv = new byte[msg.Length];
            bool alive = await Protocol.ReadExactAsync(
                c3.GetStream(), recv, 0, recv.Length, cts3.Token);
            var (respId, respPayload) = Protocol.DecodeFrame(recv);
            bool verified = alive && respId == 999 && respPayload.AsSpan().SequenceEqual(payload);

            if (!verified)
            {
                Console.WriteLine(" ✘ FAIL: Server trả về sai dữ liệu!");
                throw new InvalidOperationException("FAULT TEST FAILED: Server không phản hồi đúng hoặc sai dữ liệu.");
            }

            Console.WriteLine(" ✔ PASS: Server hoạt động bình thường, khớp RequestId=999 và Payload 1024B!");
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine(" ✘ FAIL: Test 3 bị quá hạn 5 giây (Server không phản hồi)!");
            throw new InvalidOperationException("FAULT TEST FAILED: Server timeout quá 5 giây.");
        }

        Console.WriteLine("\n=== KẾT THÚC KIỂM THỬ ===");
    }
}
