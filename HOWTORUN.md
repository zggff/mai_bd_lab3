## Как запустить:
запускает базы данных и создает сообщения kafka
```bash
docker compose up
```

### Трансформация в звезду
```bash
docker compose cp ./star_maker/flink.sql jobmanager:/opt/flink/job.sql
docker compose exec jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/job.sql
```

### Запуск обоих
```bash
./run_flink.sh
```

