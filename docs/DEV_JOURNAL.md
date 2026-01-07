# Nhật Ký Phát Triển & Học Tập (Development Journal)

File này ghi lại quá trình xây dựng dự án, các quyết định kỹ thuật, các lỗi gặp phải và cách giải quyết. Mục tiêu là để hiểu sâu về "Tại sao" (Why) và "Làm thế nào" (How).

---

## Ngày 1: Khởi tạo & Xây dựng Data Pipeline cơ bản

### 1. Kiểm tra Môi trường (Environment Check)
- **Hành động**: Chạy script `scripts/check_env.py` và kiểm tra Docker.
- **Vấn đề gặp phải**:
    - Máy chưa nhận lệnh `docker` dù đã cài Docker Desktop.
    - **Nguyên nhân**: Docker chưa được thêm vào biến môi trường `PATH` của Windows hoặc chưa khởi động xong.
    - **Giải pháp**: Người dùng đã khởi động Docker Desktop và kiểm tra lại bằng `docker --version`.

### 2. Thiết lập Kiến trúc (Architecture)
- **Hành động**: Tạo file `docs/data_model.mermaid` và `docs/api_contract.md`.
- **Tại sao?**: Trước khi viết code, cần hình dung dữ liệu đầu ra (Silver Layer) trông như thế nào. Nếu code trước mà không có thiết kế, ta sẽ phải sửa đi sửa lại Schema nhiều lần.
- **Quyết định quan trọng**:
    - Chọn **Delta Lake**: Để hỗ trợ cập nhật dữ liệu (Update/Merge) mà không cần viết lại toàn bộ file.
    - Làm phẳng (Flatten) JSON: Dữ liệu nguồn Yelp quá lồng ghép (`attributes` là JSON string), cần tách ra thành cột để lọc nhanh.

### 3. Ingestion Pipeline (Bronze Layer)
- **Quyết định Chiến lược**:
    - Ban đầu định chuyển sang Pandas để né lỗi cài đặt Spark trên Windows.
    - **Tuy nhiên**: Chúng ta quyết định quay lại **Spark** để phục vụ mục đích học tập.

- **Vấn đề Kỹ thuật (Spark on Windows)**:
    - Spark cần Hadoop Native Libs (`winutils.exe`) để quản lý file system trên Windows.
    - Nếu thiếu, Spark sẽ báo lỗi `java.io.IOException` hoặc `UnsatisfiedLinkError`.
    - **Check**: Cần kiểm tra biến `HADOOP_HOME` và file `bin/winutils.exe`.

---
