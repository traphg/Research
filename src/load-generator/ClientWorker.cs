using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

public readonly record struct RequestSample(int ClientId, int RequestId, double LatencyMs, string Status);

public class ClientWorker : IDisposable
{
    private readonly LoadConfig _cfg;
    private readonly int _clientId;
    private readonly byte[] _expectedPayload;
    private readonly byte[] _recvBuffer;
    private TcpClient? _client;
    private NetworkStream? _stream;
    private int _reqSeq;

    public List<double> LatenciesMs { get; } = new(20000);
    public List<RequestSample> Samples { get; } = new(20000);
    public long SuccessCount { get; private set; }
    public long ErrorCount { get; private set; }

    public ClientWorker(LoadConfig cfg, int clientId)
    {
        _cfg = cfg;
        _clientId = clientId;
        _reqSeq = _clientId * 10_000_000 + 1;

        _expectedPayload = new byte[_cfg.PayloadBytes];
        for (int i = 0; i < _expectedPayload.Length; i++)
            _expectedPayload[i] = (byte)((_clientId + i) % 256);

        _recvBuffer = new byte[Protocol.HeaderSize + _cfg.PayloadBytes];
    }

    // Pha 1: Mở kết nối TCP trước khi bấm giờ Warm-up.
    public async Task ConnectAsync()
    {
        _client = new TcpClient { NoDelay = true };
        using var cts = new CancellationTokenSource(_cfg.TimeoutMs);
        await _client.ConnectAsync(_cfg.Host, _cfg.Port, cts.Token);
        _stream = _client.GetStream();
    }

    // Pha 2: Chạy Warm-up đồng loạt cho đến đúng mốc warmupDeadlineTick
    public async Task RunWarmupAsync(Task<long> startWarmupGate)
    {
        if (_stream == null) throw new InvalidOperationException("Client chưa được kết nối.");
        long warmupDeadlineTick = await startWarmupGate;

        while (Stopwatch.GetTimestamp() < warmupDeadlineTick)
        {
            var (sent, connectionHealthy) = await SendAndVerifyOnceAsync(warmupDeadlineTick, recordMetric: false);
            if (!sent) break; // Đã chạm mốc warmupDeadlineTick trước khi gửi
            if (!connectionHealthy)
            {
                throw new InvalidOperationException($"Client #{_clientId} bị mất kết nối trong giai đoạn Warm-up.");
            }
        }
    }

    // Pha 3: Chạy Đo chính thức đồng loạt cho đến đúng mốc measureDeadlineTick
    public async Task RunMeasureAsync(Task<long> startMeasureGate)
    {
        if (_stream == null) throw new InvalidOperationException("Client chưa được kết nối.");
        long measureDeadlineTick = await startMeasureGate;

        while (Stopwatch.GetTimestamp() < measureDeadlineTick)
        {
            var (sent, connectionHealthy) = await SendAndVerifyOnceAsync(measureDeadlineTick, recordMetric: true);
            if (!sent) break; // Đã chạm hoặc vượt mốc measureDeadlineTick -> dừng ngay, không gửi request mới
            if (!connectionHealthy)
            {
                // Kết nối hỏng trong lúc đo: dừng vòng lặp của Client này để không sinh lỗi giả trên socket chết
                break;
            }
        }
    }

    // Thực hiện 1 chu kỳ Request-Response.
    // Trả về (sent: đã thực sự gửi trước deadline hay chưa, connectionHealthy: kết nối còn nguyên vẹn không).
    private async Task<(bool sent, bool connectionHealthy)> SendAndVerifyOnceAsync(long deadlineTick, bool recordMetric)
    {
        using var cts = new CancellationTokenSource(_cfg.TimeoutMs);
        int requestId = _reqSeq;
        byte[] packet = Protocol.Encode(requestId, _expectedPayload);

        // Lấy mốc thời gian ngay sau khi đã chuẩn bị xong packet và trước khi gọi WriteAsync
        long startTick = Stopwatch.GetTimestamp();
        if (startTick >= deadlineTick)
        {
            // Đã vượt quá cửa sổ thời gian cho phép -> KHÔNG gửi request này
            return (sent: false, connectionHealthy: true);
        }

        _reqSeq++;

        try
        {
            await _stream!.WriteAsync(packet.AsMemory(0, packet.Length), cts.Token);
            bool ok = await Protocol.ReadExactAsync(
                _stream, _recvBuffer, 0, packet.Length, cts.Token);
            long endTick = Stopwatch.GetTimestamp();

            // Round-trip Application Latency (ms)
            double latencyMs = (endTick - startTick) * 1000.0 / Stopwatch.Frequency;

            if (!ok)
            {
                if (recordMetric)
                {
                    ErrorCount++;
                    Samples.Add(new RequestSample(_clientId, requestId, latencyMs, "Disconnected"));
                }
                return (sent: true, connectionHealthy: false);
            }

            var (respReqId, respPayload) = Protocol.DecodeFrame(
                _recvBuffer.AsSpan(0, packet.Length));

            bool matched = (respReqId == requestId) && respPayload.AsSpan().SequenceEqual(_expectedPayload);

            if (recordMetric)
            {
                if (matched)
                {
                    LatenciesMs.Add(latencyMs);
                    SuccessCount++;
                    Samples.Add(new RequestSample(_clientId, requestId, latencyMs, "Success"));
                }
                else
                {
                    ErrorCount++;
                    Samples.Add(new RequestSample(_clientId, requestId, latencyMs, "Mismatch"));
                }
            }

            return (sent: true, connectionHealthy: matched);
        }
        catch (OperationCanceledException)
        {
            if (recordMetric)
            {
                double latencyMs = (Stopwatch.GetTimestamp() - startTick) * 1000.0 / Stopwatch.Frequency;
                ErrorCount++;
                Samples.Add(new RequestSample(_clientId, requestId, latencyMs, "Timeout"));
            }
            return (sent: true, connectionHealthy: false);
        }
        catch
        {
            if (recordMetric)
            {
                double latencyMs = (Stopwatch.GetTimestamp() - startTick) * 1000.0 / Stopwatch.Frequency;
                ErrorCount++;
                Samples.Add(new RequestSample(_clientId, requestId, latencyMs, "TransportError"));
            }
            return (sent: true, connectionHealthy: false);
        }
    }

    public void Dispose()
    {
        _stream?.Dispose();
        _client?.Dispose();
    }
}
