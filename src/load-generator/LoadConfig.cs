using System;
using System.IO;
using System.Text.Json;

public class LoadConfig
{
    public string Host { get; set; } = "127.0.0.1";
    public int Port { get; set; } = 9000;
    public int ClientCount { get; set; } = 10;
    public int WarmupSec { get; set; } = 5;
    public int DurationSec { get; set; } = 20;
    public int PayloadBytes { get; set; } = 1024;
    public int TimeoutMs { get; set; } = 5000;

    public static LoadConfig FromFile(string path)
    {
        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
        {
            throw new FileNotFoundException($"Không tìm thấy file cấu hình: {path}");
        }

        string json = File.ReadAllText(path);
        var cfg = JsonSerializer.Deserialize<LoadConfig>(json)
            ?? throw new InvalidDataException($"Không thể đọc nội dung JSON từ file: {path}");

        cfg.Validate();
        return cfg;
    }

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(Host))
            throw new ArgumentException("Host không được để trống.");
        if (Port < 1 || Port > 65535)
            throw new ArgumentException($"Port phải từ 1 đến 65535 (hiện tại: {Port}).");
        if (ClientCount <= 0)
            throw new ArgumentException($"ClientCount phải > 0 (hiện tại: {ClientCount}).");
        if (WarmupSec <= 0)
            throw new ArgumentException($"WarmupSec phải > 0 (hiện tại: {WarmupSec}).");
        if (DurationSec <= 0)
            throw new ArgumentException($"DurationSec phải > 0 (hiện tại: {DurationSec}).");
        if (PayloadBytes < 1 || PayloadBytes > Protocol.MaxPayloadSize)
            throw new ArgumentException($"PayloadBytes phải từ 1 đến {Protocol.MaxPayloadSize} (hiện tại: {PayloadBytes}).");
        if (TimeoutMs <= 0)
            throw new ArgumentException($"TimeoutMs phải > 0 (hiện tại: {TimeoutMs}).");
    }
}
