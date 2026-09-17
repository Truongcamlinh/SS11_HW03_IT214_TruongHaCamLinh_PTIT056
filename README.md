# SS11 HW03 - Fan-out va Consumer Group trong Kafka

**Sinh vien:** Truong Ha Cam Linh  
**Lop:** IT214  
**Ma sinh vien:** PTIT056

## 1. Mo hinh bai lam

Project gom ba module:

```text
order-producer     -> gui order.created vao storex-order-events
inventory-service -> tru kho
loyalty-service   -> cong diem thanh vien
```

Topic `storex-order-events` co 3 partition. Inventory va Loyalty doc cung topic nhung thuoc hai consumer group khac nhau:

```text
storex-order-events
  |-- inventory-group: inventory-1, inventory-2, inventory-3
  `-- loyalty-group: loyalty-service
```

Moi consumer group co offset rieng, vi vay ca Inventory va Loyalty deu nhan duoc 100% su kien. Ben trong `inventory-group`, Kafka chia cac partition cho ba instance de moi don chi bi tru kho mot lan.

## 2. Phan tich BUG-04

Neu ca hai service deu dat `group-id: storex-system`, Kafka xem chung la cac consumer cung mot nhom. Trong mot consumer group, moi partition tai mot thoi diem chi duoc giao cho mot consumer. Vi the mot message co the duoc Inventory nhan, message khac lai do Loyalty nhan. Ket qua la co don bi tru kho nhung khong cong diem, hoac cong diem nhung khong tru kho.

Day khong phai fan-out. Consumer group dung de **chia tai trong cung mot nghiep vu**, khong dung de gom cac nghiep vu khac nhau.

Cau hinh dung:

```yaml
# inventory-service
spring.kafka.consumer.group-id: inventory-group

# loyalty-service
spring.kafka.consumer.group-id: loyalty-group
```

Ba instance Inventory van phai cung dung `inventory-group`. Nho vay chung chia message voi nhau, con `loyalty-group` van co ban sao logic cua toan bo message.

## 3. REQ-01 - So partition toi thieu

De 3 instance Inventory cung hoat dong, topic can **toi thieu 3 partition**. Kafka chi gan mot partition cho toi da mot consumer trong cung group tai mot thoi diem. Neu topic chi co 2 partition thi instance thu ba se khong duoc gan partition va phai cho.

Voi 3 partition va 3 instance, moi instance thuong nhan mot partition. Khi key phan bo tuong doi deu, tai xu ly xap xi 33% cho moi instance. Kafka khong cam ket chinh xac 33% vi so message tren tung partition co the chenh lech.

Neu sau nay muon scale toi 6 Inventory instance thi nen tao topic co it nhat 6 partition. Tang partition lam thay doi phep anh xa key, nen can du tru scale ngay khi thiet ke topic.

## 4. Chay bai

Can Kafka tai `localhost:9092`. Producer tu tao topic 3 partition khi khoi dong.

```bash
./gradlew :order-producer:bootRun
./gradlew :loyalty-service:bootRun

INSTANCE_ID=inventory-1 SERVER_PORT=8081 ./gradlew :inventory-service:bootRun
INSTANCE_ID=inventory-2 SERVER_PORT=8082 ./gradlew :inventory-service:bootRun
INSTANCE_ID=inventory-3 SERVER_PORT=8083 ./gradlew :inventory-service:bootRun
```

Sau khi ba Inventory instance vao cung group, Kafka se rebalance va gan moi partition cho mot instance.

## 5. Gui message thu nghiem

```bash
curl -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -d '{"orderId":"ORD-1001","customerId":"CUS-01","totalAmount":500000}'
```

Gui nhieu don voi `orderId` khac nhau de thay log duoc chia cho ba Inventory. Moi don dong thoi xuat hien mot lan trong log Loyalty.

## 6. Ket qua mong doi

- Inventory va Loyalty deu nhan du 100% cac don hang theo nghiep vu cua minh.
- Moi don chi duoc mot Inventory instance trong `inventory-group` xu ly.
- Ba Inventory instance duoc gan ba partition va chia tai gan deu.
- Khi mot Inventory dung, Kafka rebalance partition cho hai instance con.

## 7. Build

```bash
./gradlew clean build
```
