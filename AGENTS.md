# AGENTS.md — Правила разработки проекта «Оркестратор оценки логистических рисков»

Этот файл задаёт жёсткие архитектурные инварианты и ограничения стека. Следуй им во всех задачах, не отклоняясь от них без явного указания пользователя.

---

## 1. Стек и зависимости (строгий запрет на лишние пакеты)

- **Python:** `>=3.11,<4.0`, зафиксирован в `.python-version`.
- **Менеджер окружения:** строго `uv` (`pyproject.toml`, `uv.lock`). При обновлении зависимостей пересоздавай `requirements.txt` командой `uv export --no-hashes -o requirements.txt`.
- **Единственная runtime-зависимость:** `nicegui>=3.18.0,<4.0.0`.
- **Dev-зависимости:** `pytest`, `ruff>=0.8.0`.
- **ЗАПРЕЩЕНО добавлять и импортировать:**
  - `plotly`, `narwhals` (для Ганта используется встроенный `ui.echart`).
  - `aiosqlite` (используется стандартный `sqlite3`).
  - `pandas`, `numpy`, `faker` (все генераторы и расчёты пишутся на стандартной библиотеке Python).
  - `pywebview` / `native=True`, Docker, Redis, внешние СУБД.

---

## 2. Инварианты SQLite (`core/schema.sql`, `core/storage.py`)

- Используй **одно постоянное соединение** стандартного `sqlite3` в режиме WAL.
- **Критичное правило инициализации:** открывай соединение строго с `isolation_level=None` (иначе Python неявно открывает транзакцию и SQLite молча игнорирует `PRAGMA foreign_keys = ON`).
- После открытия соединения выполняй PRAGMA строго в этом порядке:
  1. `PRAGMA journal_mode = WAL;`
  2. `PRAGMA synchronous = NORMAL;`
  3. `PRAGMA busy_timeout = 5000;`
  4. `PRAGMA foreign_keys = ON;`
- После вызова `conn.executescript(...)` **повторно выполняй** `PRAGMA foreign_keys = ON;`.
- Всеми многооператорными записями управляй явно через `BEGIN` / `COMMIT` / `ROLLBACK`.
- **Схема БД (`core/schema.sql`) неприкосновенна по структуре:**
  - 4 логических слоя, **15 таблиц**:
    - Метаданные: `processes`, `data_sources`, `process_source_rules`.
    - Пользователи и расписание: `users`, `user_sessions`, `reglament_schedules`.
    - Операционный слой: `simulation_runs`, `task_runs`, `lock_audit`, `orchestrator_events`.
    - Доменный слой: `dom_warehouse_stocks`, `dom_transit_telemetry`, `dom_weather_traffic`, `dom_supplier_sla`, `dom_sla_staging_buffer`.
  - **10 индексов**, включая `idx_task_runs_status_prio` и частичный индекс идемпотентности `idx_task_runs_idemp` (`WHERE status IN ('QUEUED', 'RUNNING')`).
  - **3 аналитических VIEW:** `v_gantt_timeline`, `v_before_after_comparison`, `v_conflict_matrix` (при дефолтном сиде должна возвращать ровно **24** пересечения).
- Не дублируй аналитические агрегаты в Python-коде, если они уже вычисляются в `v_gantt_timeline`, `v_before_after_comparison` и `v_conflict_matrix`.

---

## 3. Доменные контракты и логика оркестрации (`core/`, `services/`)

### 3.1. Декларативность правил (FR-01)
- `RWLockManager` и планировщик **не должны хардкодить** матрицу конфликтов в коде. Все ключи блокировок, режимы доступа (`READ` / `WRITE` / `READ_WRITE`), область (`BY_DATE` / `GLOBAL`), `allow_parallel_dates`, `uses_shared_staging`, `staging_key`, `acquire_order` и `timeout_sec` читаются из `processes` и `process_source_rules`.
- Базовые параметры 4 процессов:
  - `StockoutRisk`: `allow_parallel_dates = 0`.
  - `DelayRisk`: `allow_parallel_dates = 1`.
  - `SpoilageRisk`: `allow_parallel_dates = 1`.
  - `CarrierDefaultRisk`: `allow_parallel_dates = 0`, `uses_shared_staging = 1`, `staging_key = 'staging_carrier_sla_rolling'`.
- Базовая гранулярность 4 витрин:
  - `WarehouseStocks`, `TransitTelemetry`, `WeatherAndTraffic` — `DATE_PARTITIONED` (`lock_scope = 'BY_DATE'`).
  - `SupplierSLA` — `GLOBAL_CUMULATIVE` (`lock_scope = 'GLOBAL'`).

### 3.2. Атомарные блокировки и TTL (FR-02, NFR-02)
- Проверка совместимости и захват **всех** ключей задачи выполняются атомарно под единым `asyncio.Lock`.
- `SHARED` совместим с `SHARED`; `EXCLUSIVE` конфликтует с любым активным замком.
- **Запрещён частичный захват:** если занят хотя бы один ключ, задача не удерживает остальные ключи, остаётся в `QUEUED`, заполняет `task_runs.blocked_by` и пишет `WAIT_CONFLICT` в `lock_audit`.
- При превышении `timeout_sec` (по умолчанию 30 с) ключи принудительно освобождаются с записью `EXPIRED_TTL` в `lock_audit` и переводом задачи в `FAILED` (`error_type = 'TimeoutError'`).

### 3.3. Очередь, идемпотентность и аудит (FR-03, FR-04, FR-05)
- Приоритеты: `AUTO = 1`, `MANUAL = 2..3`.
- Очередь строго **work-conserving**: заблокированная высокоприоритетная задача не тормозит независимые задачи ниже по очереди.
- Реализован **Priority Aging**: ожидающие ручные задачи со временем повышают `effective_priority`.
- Активный дубль `(process_id, target_date)` при наличии задачи в `QUEUED`/`RUNNING` сразу получает статус `DEDUPLICATED` (при `orchestration_enabled = 1`).
- `cancel(task_id)` переводит задачу из `QUEUED` в `CANCELLED`.
- Каждая задача хранит `initiated_by` и `session_id`; все переходы фиксируются в `task_runs`, `lock_audit`, `orchestrator_events` и агрегируются в `simulation_runs`.

### 3.4. Режим сравнения «До / После» (FR-06)
- Переключатель `orchestration_enabled` (`0` / `1`) управляет прохождением через оркестратор, **не изменяя формулы расчёта рисков** в `services/microservices.py`.
- При `orchestration_enabled = 0` должны детерминированно воспроизводиться все 4 класса сбоев:
  - `DeadlockError` (AB-BA по `acquire_order` между `StockoutRisk` и `DelayRisk` на одной дате).
  - `DataCorruptionError` — Lost Update по проверке `row_version` в `WarehouseStocks`.
  - `CrossDateCollisionError` — коллизия `owner_task_id` в `dom_sla_staging_buffer`.
  - `DataCorruptionError` — грязное чтение глобальной витрины `SupplierSLA`.
- При `orchestration_enabled = 1` на всех 5 канонических сценариях: **0 deadlock**, **0 ошибок записи**, `s4_parallel_safe` выполняется параллельно.

---

## 4. Правила UI на NiceGUI 3.x (`ui/`, `main.py`)

- **Только браузерный режим:** `ui.run(..., native=False)` на `localhost:8080`. Никогда не используй `native=True`.
- **Изоляция пользовательских вкладок:** весь пользовательский интерфейс строится внутри `@ui.page('/')`; глобальные разделяемые объекты (`Orchestrator`, `Storage`) хранятся на уровне приложения, а клиентские состояния и активный пользователь — на уровне сессии вкладки (`user_sessions`).
- **Защита от утечек памяти (NFR-04):** подписки UI на события оркестратора реализуй через `nicegui.Event()` либо с обязательной отпиской в `ui.context.client.on_delete(...)`. Не храни «вечные» ссылки на UI-элементы закрытых клиентов.
- **Диаграмма Ганта (`ui/components/gantt_chart.py`):**
  - Строго через нативный `ui.echart` (Apache ECharts Stacked Bar с прозрачной серией `Смещение`, жёлтой серией ожидания и цветной серией выполнения).
  - Чистый Python-словарь `options`, **без кастомного JavaScript** (`:renderItem` и т. п.) и без Plotly.
- **Запуск одной командой (NFR-05):** `main.py` при старте обязан проверять наличие `synthetic_data/*.csv`, `synthetic_data/scenarios.json` и `data/orchestrator.db`; если чего-то нет — автоматически прогонять генераторы (`seed=42`) и инициализировать БД, чтобы чистый клон запускался командой `uv run main.py`.

---

## 5. Контрольные цифры синтетики (`seed=42`)

Любые изменения в `scripts/` не должны ломать воспроизводимость контрольных метрик при `seed=42`:

- **Доменные витрины (`scripts/generate_domain_data.py`):** ровно **316** строк:
  - `warehouse_stocks.csv`: 200 строк (6 аномалий stockout в Москве/Казани на `2026-11-20`, 6 аномалий spoilage в Краснодаре на `2026-11-20..21`).
  - `transit_telemetry.csv`: 90 строк (31 рейс со статусом `DELAYED`/`STOPPED`).
  - `weather_traffic.csv`: 20 строк.
  - `supplier_sla.csv`: 6 строк (4 перевозчика в зонах `C`/`CRITICAL`, включая `CAR-04` и `CAR-06`).
- **Логи старой системы (`scripts/generate_legacy_logs.py`):** ровно **86** событий (`51 INFO`, `14 WARN`, `21 ERROR`) по **38** задачам (55,3% аварийных):
  - `ERR_DEADLOCK_ABBA`: 8
  - `ERR_CROSS_DATE_STAGING`: 6
  - `ERR_LOST_UPDATE`: 4
  - `ERR_DIRTY_READ_GLOBAL`: 3
- **Сценарии (`scripts/generate_scenarios.py`):** 5 канонических сценариев (`s1_manual_vs_reglament` .. `s5_multiuser_storm` на 10 задач) + `random_stress_batch` (15 задач). При загрузке строковый `trigger` конвертируется в `TriggerType(task["trigger"])`.

---

## 6. Команды проверки перед завершением каждой задачи

После каждого значимого изменения запускай и проверяй отсутствие ошибок:

```bash
uv run ruff check .
uv run pytest -v
```

При изменении БД дополнительно проверяй:
- `PRAGMA foreign_keys` == `1` и `PRAGMA journal_mode` == `'wal'`.
- Среднее время одиночной операции записи/чтения SQLite `< 2 мс` (NFR-03).
- Удаление `data/` и `synthetic_data/` с последующим `uv run main.py` корректно восстанавливает стенд с нуля.