# Event-Driven Architecture Demo  
## Django + Kafka + Celery

🚀 Demo kiến trúc **Event-Driven Architecture (EDA)** sử dụng **Django REST API**, **Apache Kafka**, **Celery** và **Redis** để xử lý bất đồng bộ, mở rộng và tách biệt các service.

---

## 📌 Architecture Overview

**Flow tổng quát:**

1. Client gửi request tạo Order → Django API
2. Django trả response ngay (non-blocking)
3. Celery publish event vào Kafka
4. Kafka phân phối message tới nhiều Consumer độc lập
5. Mỗi Consumer xử lý một nghiệp vụ riêng
6. Order được cập nhật trạng thái khi hoàn tất

**Consumers:**
- Inventory Service
- Notification Service
- Analytics Service

---

## 🧩 Tech Stack

- **Backend:** Django, Django REST Framework
- **Message Broker:** Apache Kafka
- **Async Tasks:** Celery
- **Cache / Queue:** Redis
- **Containerization:** Docker, Docker Compose
- **Monitoring:** Kafka UI

---
