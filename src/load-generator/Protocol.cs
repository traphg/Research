using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

public static class Protocol
{
    // Cấu trúc Frame (Big Endian):
    // [0..3]   : BodyLength (4 bytes) = Số byte đứng sau BodyLength (4 bytes RequestId + Payload.Length)
    // [4..7]   : RequestId  (4 bytes)
    // [8..N]   : Payload    (Mặc định 1024 bytes)
    // -> HeaderSize = 8 bytes; Với Payload 1024 bytes thì BodyLength = 1028, Tổng Frame = 1032 bytes.
    public const int LengthFieldSize = 4;
    public const int RequestIdSize = 4;
    public const int HeaderSize = LengthFieldSize + RequestIdSize; // 8 bytes

    public const int MaxPayloadSize = 10 * 1024; // Giới hạn Payload tối đa 10 KB (10240 bytes)
    public const int MaxBodyLength = RequestIdSize + MaxPayloadSize; // 10244 bytes

    public static byte[] Encode(int requestId, byte[] payload)
    {
        int bodyLength = RequestIdSize + payload.Length;
        int totalFrameLength = LengthFieldSize + bodyLength;

        byte[] buffer = new byte[totalFrameLength];
        BinaryPrimitives.WriteInt32BigEndian(buffer.AsSpan(0, 4), bodyLength);
        BinaryPrimitives.WriteInt32BigEndian(buffer.AsSpan(4, 4), requestId);
        Buffer.BlockCopy(payload, 0, buffer, HeaderSize, payload.Length);
        return buffer;
    }

    public static (int requestId, byte[] payload) DecodeFrame(ReadOnlySpan<byte> fullFrame)
    {
        int requestId = BinaryPrimitives.ReadInt32BigEndian(fullFrame.Slice(4, 4));
        byte[] payload = fullFrame.Slice(HeaderSize).ToArray();
        return (requestId, payload);
    }

    // Đọc lặp cho đến khi đủ count bytes. Trả về false nếu kết nối đóng (ReadAsync == 0).
    public static async Task<bool> ReadExactAsync(
        Stream stream, byte[] buffer, int offset, int count, CancellationToken ct)
    {
        int totalRead = 0;
        while (totalRead < count)
        {
            int n = await stream.ReadAsync(
                buffer.AsMemory(offset + totalRead, count - totalRead), ct);
            if (n == 0) return false;
            totalRead += n;
        }
        return true;
    }
}