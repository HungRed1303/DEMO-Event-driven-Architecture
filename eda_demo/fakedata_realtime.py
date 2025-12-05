"""
Gửi nhiều requests đồng thời để test real-time processing
Sử dụng ThreadPoolExecutor để gửi parallel requests
"""
import requests
import time
import random
from faker import Faker
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

fake = Faker()

API_URL = "http://localhost:8000/orders/"

# Cấu hình
NUM_MESSAGES = 100  # Tổng số messages
NUM_WORKERS = 10    # Số threads chạy đồng thời
BATCH_SIZE = 20     # Gửi theo batch để dễ quan sát

def send_single_request(request_id):
    """
    Gửi 1 request đến API
    
    Args:
        request_id: ID để tracking
        
    Returns:
        dict: Kết quả request
    """
    data = {
        "product": fake.word().capitalize(),
        "quantity": random.randint(1, 10)
    }
    
    # Tạo idempotency key (optional - để test duplicate prevention)
    idempotency_key = f"req-{request_id}-{int(time.time())}"
    
    headers = {
        "Content-Type": "application/json",
        "X-Idempotency-Key": idempotency_key
    }
    
    start_time = time.time()
    
    try:
        response = requests.post(API_URL, json=data, headers=headers, timeout=10)
        elapsed = time.time() - start_time
        
        result = {
            "request_id": request_id,
            "status_code": response.status_code,
            "elapsed_time": round(elapsed, 3),
            "success": response.status_code in [200, 201],
            "data": response.json() if response.status_code in [200, 201] else None,
            "error": None
        }
        
        print(f"✅ Request #{request_id}: {response.status_code} - {elapsed:.3f}s - {data['product']}")
        return result
        
    except requests.exceptions.Timeout:
        elapsed = time.time() - start_time
        print(f"⏱️  Request #{request_id}: TIMEOUT after {elapsed:.3f}s")
        return {
            "request_id": request_id,
            "status_code": None,
            "elapsed_time": round(elapsed, 3),
            "success": False,
            "data": None,
            "error": "Timeout"
        }
        
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"❌ Request #{request_id}: ERROR - {str(e)}")
        return {
            "request_id": request_id,
            "status_code": None,
            "elapsed_time": round(elapsed, 3),
            "success": False,
            "data": None,
            "error": str(e)
        }

def send_batch(batch_num, batch_size, num_workers):
    """
    Gửi 1 batch requests đồng thời
    
    Args:
        batch_num: Số thứ tự batch
        batch_size: Số requests trong batch
        num_workers: Số threads chạy đồng thời
    """
    print(f"\n{'='*70}")
    print(f"📦 BATCH {batch_num}: Sending {batch_size} requests with {num_workers} workers")
    print(f"{'='*70}")
    
    batch_start = time.time()
    results = []
    
    # Sử dụng ThreadPoolExecutor để gửi concurrent requests
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Submit tất cả requests
        futures = []
        for i in range(batch_size):
            request_id = (batch_num - 1) * batch_size + i + 1
            future = executor.submit(send_single_request, request_id)
            futures.append(future)
        
        # Đợi tất cả requests hoàn thành
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
    
    batch_elapsed = time.time() - batch_start
    
    # Thống kê
    success_count = sum(1 for r in results if r["success"])
    failed_count = len(results) - success_count
    avg_time = sum(r["elapsed_time"] for r in results) / len(results)
    max_time = max(r["elapsed_time"] for r in results)
    min_time = min(r["elapsed_time"] for r in results)
    
    print(f"\n{'='*70}")
    print(f"📊 BATCH {batch_num} SUMMARY:")
    print(f"   Total requests: {len(results)}")
    print(f"   ✅ Success: {success_count}")
    print(f"   ❌ Failed: {failed_count}")
    print(f"   ⏱️  Batch time: {batch_elapsed:.3f}s")
    print(f"   📈 Avg response time: {avg_time:.3f}s")
    print(f"   ⚡ Min response time: {min_time:.3f}s")
    print(f"   🐌 Max response time: {max_time:.3f}s")
    print(f"   🚀 Throughput: {len(results)/batch_elapsed:.2f} req/s")
    print(f"{'='*70}\n")
    
    return results

def main():
    """Main function để chạy demo"""
    print(f"""
╔═══════════════════════════════════════════════════════════════╗
║           CONCURRENT REQUESTS LOAD TEST                       ║
╚═══════════════════════════════════════════════════════════════╝

Configuration:
  • Total messages: {NUM_MESSAGES}
  • Concurrent workers: {NUM_WORKERS}
  • Batch size: {BATCH_SIZE}
  • API endpoint: {API_URL}

Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    """)
    
    overall_start = time.time()
    all_results = []
    
    # Tính số batch cần gửi
    num_batches = (NUM_MESSAGES + BATCH_SIZE - 1) // BATCH_SIZE
    
    for batch_num in range(1, num_batches + 1):
        # Batch cuối có thể ít hơn BATCH_SIZE
        remaining = NUM_MESSAGES - (batch_num - 1) * BATCH_SIZE
        current_batch_size = min(BATCH_SIZE, remaining)
        
        batch_results = send_batch(batch_num, current_batch_size, NUM_WORKERS)
        all_results.extend(batch_results)
        
        # Delay giữa các batch để dễ quan sát
        if batch_num < num_batches:
            print(f"⏳ Waiting 3 seconds before next batch...\n")
            time.sleep(3)
    
    overall_elapsed = time.time() - overall_start
    
    # Tổng kết cuối cùng
    total_success = sum(1 for r in all_results if r["success"])
    total_failed = len(all_results) - total_success
    overall_avg_time = sum(r["elapsed_time"] for r in all_results) / len(all_results)
    
    print(f"""
╔═══════════════════════════════════════════════════════════════╗
║                    OVERALL SUMMARY                            ║
╚═══════════════════════════════════════════════════════════════╝

Total Statistics:
  • Total requests sent: {len(all_results)}
  • ✅ Successful: {total_success} ({total_success/len(all_results)*100:.1f}%)
  • ❌ Failed: {total_failed} ({total_failed/len(all_results)*100:.1f}%)
  • ⏱️  Total time: {overall_elapsed:.2f}s
  • 📈 Average response time: {overall_avg_time:.3f}s
  • 🚀 Overall throughput: {len(all_results)/overall_elapsed:.2f} req/s

Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    """)
    
    # Lưu kết quả ra file (optional)
    try:
        with open('load_test_results.txt', 'w') as f:
            f.write(f"Load Test Results - {datetime.now()}\n")
            f.write(f"Total: {len(all_results)}, Success: {total_success}, Failed: {total_failed}\n\n")
            for r in all_results:
                f.write(f"Request {r['request_id']}: {r['status_code']} - {r['elapsed_time']}s\n")
        print("📝 Results saved to load_test_results.txt")
    except Exception as e:
        print(f"⚠️  Could not save results: {e}")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Fatal error: {e}")