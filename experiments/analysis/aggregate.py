import math
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Cấu hình font chữ để hiển thị tiếng Việt mượt mà trên Windows
plt.rcParams['font.sans-serif'] = ['Segoe UI', 'Arial', 'DejaVu Sans']

def nearest_rank_p95(values):
    s = sorted(values)
    if not s:
        return float("nan")
    idx = math.ceil(0.95 * len(s)) - 1
    return s[max(0, idx)]

def main():
    results_path = "experiments/raw-data/run_results.csv"
    samples_path = "experiments/raw-data/latency_samples.csv"

    if not os.path.exists(results_path):
        raise FileNotFoundError(f"Không tìm thấy file dữ liệu tổng hợp: {results_path}")
    if not os.path.exists(samples_path):
        raise FileNotFoundError(f"Không tìm thấy file dữ liệu chi tiết từng request: {samples_path}")

    df = pd.read_csv(results_path)
    sdf = pd.read_csv(samples_path)

    # 1. KIỂM TRA TÍNH ĐẦY ĐỦ VÀ HỢP LỆ CỦA BỘ DỮ LIỆU THỰC NGHIỆM (9 LẦN CHẠY)
    expected_columns = [
        "RunId", "TimestampUtc", "ClientCount", "Repeat", "PayloadBytes",
        "WarmupSeconds", "MeasureSeconds", "ActualMeasureSeconds",
        "TotalRequests", "SuccessRequests", "ErrorRequests",
        "AvgLatencyMs", "P95LatencyMs", "ThroughputRps", "ErrorRatePercent", "ServerCpuPercent"
    ]
    missing_cols = [c for c in expected_columns if c not in df.columns]
    if missing_cols:
        raise ValueError(f"File run_results.csv thiếu các cột bắt buộc: {missing_cols}")

    expected_sample_columns = ["RunId", "ClientId", "RequestId", "LatencyMs", "Status"]
    missing_sample_cols = [c for c in expected_sample_columns if c not in sdf.columns]
    if missing_sample_cols:
        raise ValueError(f"File latency_samples.csv thiếu các cột bắt buộc: {missing_sample_cols}")

    if len(df) != 9:
        raise ValueError(f"Yêu cầu đúng 9 lần chạy chính thức (3 mức x 3 lần lặp), nhưng hiện có {len(df)} dòng trong run_results.csv.")

    if not df["RunId"].is_unique:
        raise ValueError("Phát hiện RunId bị trùng lặp trong run_results.csv! Mỗi lần chạy phải có RunId duy nhất.")

    # Kiểm tra tập RunId khớp chính xác giữa 2 file CSV (chặn triệt để dữ liệu cũ còn sót)
    results_ids = set(df["RunId"])
    sample_ids = set(sdf["RunId"])
    if results_ids != sample_ids:
        raise ValueError(
            f"RunId trong hai file CSV không khớp! run_results.csv có {results_ids}, latency_samples.csv có {sample_ids}. "
            "Có thể latency_samples.csv chứa dữ liệu cũ từ các lần chạy trước, vui lòng xóa sạch thư mục experiments/raw-data/ trước khi chạy chính thức."
        )

    client_levels = set(df["ClientCount"].unique())
    if client_levels != {1, 10, 50}:
        raise ValueError(f"Các mức ClientCount phải đúng là {{1, 10, 50}}, nhưng thực tế là {client_levels}.")

    for level, group in df.groupby("ClientCount"):
        repeats = set(group["Repeat"].unique())
        if len(group) != 3 or repeats != {1, 2, 3}:
            raise ValueError(f"Mức ClientCount={level} phải có đúng 3 lần chạy với Repeat={{1, 2, 3}}, nhưng thực tế là {repeats}.")

    if (df["ActualMeasureSeconds"] <= 0).any():
        raise ValueError("Có lần chạy có ActualMeasureSeconds <= 0.")

    if (df["TotalRequests"] <= 0).any():
        raise ValueError("Có lần chạy có TotalRequests <= 0.")

    # 2. KIỂM CHỨNG ĐỘC LẬP TỪ run_results.csv VÀ latency_samples.csv (7 TIÊU CHÍ)
    df["Verified_TotalRequests"] = df["SuccessRequests"] + df["ErrorRequests"]
    df["Verified_ThroughputRps"] = (df["SuccessRequests"] / df["ActualMeasureSeconds"]).round(2)
    df["Verified_ErrorRatePercent"] = (df["ErrorRequests"] / df["Verified_TotalRequests"] * 100.0).round(4)

    sample_counts = sdf.groupby("RunId").agg(
        Sample_TotalRequests=("RequestId", "count"),
        Sample_SuccessRequests=("Status", lambda s: (s == "Success").sum()),
        Sample_ErrorRequests=("Status", lambda s: (s != "Success").sum())
    ).reset_index()

    sdf_ok = sdf[sdf["Status"] == "Success"]
    sample_lat = sdf_ok.groupby("RunId")["LatencyMs"].agg(
        Sample_AvgLatencyMs="mean",
        Sample_P95LatencyMs=lambda x: nearest_rank_p95(x.tolist())
    ).reset_index().round(4)

    df = df.merge(sample_counts, on="RunId", how="left").merge(sample_lat, on="RunId", how="left")

    if df["Sample_TotalRequests"].isna().any() or (df["Sample_TotalRequests"] <= 0).any():
        raise ValueError("Có RunId trong run_results.csv không có dữ liệu request tương ứng trong latency_samples.csv!")

    df["Sample_AvgLatencyMs"] = df["Sample_AvgLatencyMs"].round(4)
    df["Sample_P95LatencyMs"] = df["Sample_P95LatencyMs"].round(4)

    df["TotalReq_OK"] = (df["TotalRequests"] == df["Verified_TotalRequests"]) & (df["TotalRequests"] == df["Sample_TotalRequests"])
    df["SuccessCount_OK"] = (df["SuccessRequests"] == df["Sample_SuccessRequests"])
    df["ErrorCount_OK"] = (df["ErrorRequests"] == df["Sample_ErrorRequests"])
    df["Throughput_OK"] = (df["ThroughputRps"] - df["Verified_ThroughputRps"]).abs() <= 0.05
    df["ErrorRate_OK"] = (df["ErrorRatePercent"] - df["Verified_ErrorRatePercent"]).abs() <= 0.001
    df["AvgLatency_OK"] = (df["AvgLatencyMs"] - df["Sample_AvgLatencyMs"]).abs() <= 0.005
    df["P95Latency_OK"] = (df["P95LatencyMs"] - df["Sample_P95LatencyMs"]).abs() <= 0.005

    all_checks = [
        "TotalReq_OK",
        "SuccessCount_OK",
        "ErrorCount_OK",
        "Throughput_OK",
        "ErrorRate_OK",
        "AvgLatency_OK",
        "P95Latency_OK"
    ]

    os.makedirs("experiments/analysis", exist_ok=True)
    df.to_csv("experiments/analysis/verification_check.csv", index=False)
    print("=== BẢNG KIỂM CHỨNG ĐỘC LẬP 9 LẦN CHẠY (VERIFICATION CHECK) ===")
    print(df[["RunId", "ClientCount"] + all_checks])

    # Dừng ngay lập tức nếu bất kỳ tiêu chí kiểm chứng nào sai
    if not df[all_checks].all().all():
        raise ValueError(
            "Dữ liệu kiểm chứng không khớp trong verification_check.csv! "
            "Không được sử dụng kết quả này để phân tích và vẽ biểu đồ."
        )

    # 3. TÍNH TRUNG BÌNH (MEAN) VÀ ĐỘ LỆCH CHUẨN (STD) CHO ĐỦ 5 CHỈ SỐ
    summary = df.groupby("ClientCount").agg(
        AvgLatency_Mean=("AvgLatencyMs", "mean"),
        AvgLatency_Std=("AvgLatencyMs", "std"),
        P95Latency_Mean=("P95LatencyMs", "mean"),
        P95Latency_Std=("P95LatencyMs", "std"),
        Throughput_Mean=("ThroughputRps", "mean"),
        Throughput_Std=("ThroughputRps", "std"),
        ErrorRate_Mean=("ErrorRatePercent", "mean"),
        ErrorRate_Std=("ErrorRatePercent", "std"),
        ServerCpu_Mean=("ServerCpuPercent", "mean"),
        ServerCpu_Std=("ServerCpuPercent", "std")
    ).reset_index().round(4)

    summary.to_csv("experiments/analysis/summary.csv", index=False)
    print("\n=== BẢNG TỔNG HỢP THỐNG KÊ 5 CHỈ SỐ (MEAN & STD) ===")
    print(summary)

    # 4. VẼ 3 BIỂU ĐỒ RIÊNG BIỆT LƯU VÀO experiments/figures/
    os.makedirs("experiments/figures", exist_ok=True)
    x_labels = summary["ClientCount"].astype(str)
    x_pos = np.arange(len(x_labels))

    # Biểu đồ 1: Round-trip Application Latency (Average vs. P95)
    plt.figure(figsize=(8, 5))
    width = 0.35
    plt.bar(x_pos - width/2, summary["AvgLatency_Mean"], width=width, yerr=summary["AvgLatency_Std"],
            capsize=4, label="Average Latency (ms)", color="#2b5c8f")
    plt.bar(x_pos + width/2, summary["P95Latency_Mean"], width=width, yerr=summary["P95Latency_Std"],
            capsize=4, label="P95 Latency (ms)", color="#d95f02")
    plt.title("Biểu đồ 1: Ảnh hưởng của số lượng Client đến Round-trip Application Latency")
    plt.xlabel("Số lượng Client đồng thời")
    plt.ylabel("Round-trip Application Latency (ms)")
    plt.xticks(x_pos, x_labels)
    plt.grid(axis="y", linestyle="--", alpha=0.6)
    plt.legend()
    plt.tight_layout()
    plt.savefig("experiments/figures/latency.png", dpi=300)
    plt.close()

    # Biểu đồ 2: Throughput (Requests / second)
    plt.figure(figsize=(8, 5))
    plt.bar(x_pos, summary["Throughput_Mean"], width=0.45, yerr=summary["Throughput_Std"],
            capsize=5, color="#388e3c", label="Throughput (requests/sec)")
    plt.title("Biểu đồ 2: Ảnh hưởng của số lượng Client đến Thông lượng (Throughput)")
    plt.xlabel("Số lượng Client đồng thời")
    plt.ylabel("Throughput (requests/sec)")
    plt.xticks(x_pos, x_labels)
    plt.grid(axis="y", linestyle="--", alpha=0.6)
    plt.legend()
    plt.tight_layout()
    plt.savefig("experiments/figures/throughput.png", dpi=300)
    plt.close()

    # Biểu đồ 3: Server CPU Usage (%)
    plt.figure(figsize=(8, 5))
    plt.plot(x_labels, summary["ServerCpu_Mean"], marker="o", linewidth=2.5,
             color="#c62828", label="Server CPU Usage (%)")
    plt.errorbar(x_labels, summary["ServerCpu_Mean"], yerr=summary["ServerCpu_Std"],
                 fmt="none", ecolor="#c62828", capsize=5)
    plt.title("Biểu đồ 3: Ảnh hưởng của số lượng Client đến Mức sử dụng CPU của Server")
    plt.xlabel("Số lượng Client đồng thời")
    plt.ylabel("Server CPU Usage (%)")
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.legend()
    plt.tight_layout()
    plt.savefig("experiments/figures/cpu_usage.png", dpi=300)
    plt.close()

    print("\n[OK] Kiểm chứng thành công 7/7 tiêu chí! Đã xuất verification_check.csv, summary.csv và 3 biểu đồ!")

if __name__ == "__main__":
    main()
