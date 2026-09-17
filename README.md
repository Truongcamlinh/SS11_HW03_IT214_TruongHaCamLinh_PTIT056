# SS11 HW03 - Cơ chế Fan-out và Consumer Group trong Kafka

**Sinh viên:** Trương Hà Cẩm Linh

**Lớp:** IT214

**Mã sinh viên:** PTIT056

## 1. Mô hình bài làm

Dự án gồm ba module:

```text
order-producer     -> gửi sự kiện order.created vào storex-order-events
inventory-service -> trừ số lượng hàng trong kho
loyalty-service   -> cộng điểm thành viên
```

Topic `storex-order-events` có 3 partition. Inventory Service và Loyalty Service cùng đọc một topic nhưng thuộc hai consumer group khác nhau:

```text
storex-order-events
  |-- inventory-group: inventory-1, inventory-2, inventory-3
  `-- loyalty-group: loyalty-service
```

Mỗi consumer group quản lý offset riêng. Vì vậy, cả Inventory Service và Loyalty Service đều nhận được 100% sự kiện đơn hàng. Bên trong `inventory-group`, Kafka chia các partition cho ba instance để mỗi đơn hàng chỉ bị trừ kho một lần.

## 2. Phân tích lỗi BUG-04

Nếu Inventory Service và Loyalty Service đều đặt `group-id: storex-system`, Kafka sẽ xem chúng là các consumer thuộc cùng một nhóm. Trong một consumer group, mỗi partition tại một thời điểm chỉ được giao cho một consumer.

Do đó, một message có thể được Inventory Service nhận, trong khi message khác lại do Loyalty Service nhận. Hậu quả là có đơn hàng bị trừ kho nhưng không được cộng điểm hoặc được cộng điểm nhưng không bị trừ kho.

Đây không phải là cơ chế fan-out. Consumer group được dùng để **chia tải giữa các instance thực hiện cùng một nghiệp vụ**, không được dùng để gộp các service có nghiệp vụ khác nhau.

Cấu hình đúng:

```yaml
# inventory-service
spring.kafka.consumer.group-id: inventory-group

# loyalty-service
spring.kafka.consumer.group-id: loyalty-group
```

Ba instance Inventory Service vẫn phải dùng chung `inventory-group`. Nhờ vậy, chúng chia message với nhau, còn `loyalty-group` vẫn nhận được toàn bộ message để thực hiện cộng điểm.

## 3. REQ-01 - Số lượng partition tối thiểu

Để 3 instance Inventory Service cùng hoạt động hiệu quả, topic cần có **tối thiểu 3 partition**.

Kafka chỉ giao một partition cho tối đa một consumer trong cùng consumer group tại một thời điểm. Nếu topic chỉ có 2 partition thì instance Inventory thứ ba sẽ không được giao partition và phải ở trạng thái chờ.

Với 3 partition và 3 instance, thông thường mỗi instance được giao một partition. Khi key và số lượng message được phân bố tương đối đều, mỗi instance xử lý khoảng 33% số đơn hàng.

Kafka không cam kết chính xác mỗi instance nhận đúng 33%, vì lượng message trong từng partition có thể chênh lệch. Nếu sau này hệ thống muốn chạy tối đa 6 instance Inventory Service thì topic nên có ít nhất 6 partition.

## 4. Cơ chế Fan-out

Kafka thực hiện fan-out thông qua các consumer group khác nhau:

1. Producer gửi một message vào topic `storex-order-events`.
2. `inventory-group` nhận message để xử lý trừ kho.
3. `loyalty-group` cũng nhận chính message đó để cộng điểm.
4. Offset của hai group được lưu độc lập nên hai nghiệp vụ không tranh giành message của nhau.

Khi có nhiều instance trong `inventory-group`, Kafka chỉ giao mỗi message cho một instance của nhóm. Vì vậy hệ thống vừa bảo đảm fan-out giữa các service, vừa bảo đảm chia tải giữa các instance Inventory Service.

## 5. Cấu trúc thư mục

```text
.
|-- order-producer/     # API tạo đơn và gửi sự kiện lên Kafka
|-- inventory-service/ # Consumer trừ kho, group-id: inventory-group
|-- loyalty-service/   # Consumer cộng điểm, group-id: loyalty-group
|-- build.gradle
|-- settings.gradle
`-- README.md
```

## 6. Hướng dẫn chạy

Máy cần có Kafka đang chạy tại `localhost:9092`. Khi Order Producer khởi động, ứng dụng sẽ tạo topic `storex-order-events` gồm 3 partition nếu topic chưa tồn tại.

Khởi động Producer và Loyalty Service:

```bash
./gradlew :order-producer:bootRun
./gradlew :loyalty-service:bootRun
```

Khởi động ba instance Inventory Service ở ba cửa sổ terminal khác nhau:

```bash
INSTANCE_ID=inventory-1 SERVER_PORT=8081 ./gradlew :inventory-service:bootRun
INSTANCE_ID=inventory-2 SERVER_PORT=8082 ./gradlew :inventory-service:bootRun
INSTANCE_ID=inventory-3 SERVER_PORT=8083 ./gradlew :inventory-service:bootRun
```

Sau khi ba instance tham gia cùng `inventory-group`, Kafka thực hiện rebalance và giao mỗi partition cho một instance.

## 7. Gửi sự kiện kiểm thử

```bash
curl -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -d '{"orderId":"ORD-1001","customerId":"CUS-01","totalAmount":500000}'
```

Gửi nhiều đơn hàng có `orderId` khác nhau để quan sát log được chia cho ba Inventory Service. Mỗi đơn hàng đồng thời xuất hiện một lần trong log của Loyalty Service.

## 8. Kết quả mong đợi

- Inventory Service và Loyalty Service đều nhận đủ 100% sự kiện theo nghiệp vụ của mình.
- Mỗi đơn hàng chỉ được một instance trong `inventory-group` xử lý.
- Ba instance Inventory Service được giao ba partition và chia tải tương đối đều.
- Khi một instance Inventory dừng, Kafka tự động rebalance partition cho các instance còn lại.
- Không xảy ra tình trạng đơn hàng bị trừ kho nhưng không được cộng điểm do dùng sai group-id.

## 9. Build dự án

```bash
./gradlew clean build
```
