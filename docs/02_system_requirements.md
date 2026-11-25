# Системные требования и матрица конфликтов (RiskFlow Orchestrator)

## 1. Матрица ресурсных контрактов и конфликтов READ/WRITE

| Микросервис | Читает (`SHARED`) | Пишет (`EXCLUSIVE`) | Область блокировок | `allow_parallel_dates` | Staging-ключ | Порядок захвата в AS-IS (`acquire_order`) |
| :--- | :--- | :--- | :--- | :---: | :--- | :--- |
| `StockoutRisk` | `TransitTelemetry`, `SupplierSLA` | `WarehouseStocks` | `WarehouseStocks`, `TransitTelemetry`: `BY_DATE`; `SupplierSLA`: `GLOBAL` | `False` | — | 1: `WarehouseStocks` → 2: `TransitTelemetry` → 3: `SupplierSLA` |
| `DelayRisk` | `WarehouseStocks`, `WeatherAndTraffic`, `SupplierSLA` | `TransitTelemetry` | `TransitTelemetry`, `WarehouseStocks`, `WeatherAndTraffic`: `BY_DATE`; `SupplierSLA`: `GLOBAL` | `True` | — | 1: `TransitTelemetry` → 2: `WarehouseStocks` → 3: `WeatherAndTraffic` → 4: `SupplierSLA` |
| `SpoilageRisk` | `WeatherAndTraffic` | `WarehouseStocks` (`READ_WRITE`) | `BY_DATE` | `True` | — | 1: `WarehouseStocks` → 2: `WeatherAndTraffic` |
| `CarrierDefaultRisk` | `TransitTelemetry`, `WeatherAndTraffic` | `SupplierSLA` | `SupplierSLA`: `GLOBAL`; `TransitTelemetry`, `WeatherAndTraffic`: `BY_DATE` | `False` | `staging_carrier_sla_rolling` | 1: `SupplierSLA` → 2: `TransitTelemetry` → 3: `WeatherAndTraffic` |

---

## 2. Функциональные требования (FR)

| Код | Требование | Критерий приёмки |
| :--- | :--- | :--- |
| **FR-01** | **Декларативный реестр контрактов** в SQLite (`processes`, `data_sources`, `process_source_rules`) | Все режимы доступа (`READ`/`WRITE`), области (`BY_DATE`/`GLOBAL`), `allow_parallel_dates`, `staging_key` и `timeout_sec` читаются из БД без хардкода матрицы конфликтов в планировщике; `v_conflict_matrix` возвращает 24 пересечения. |
| **FR-02** | **Атомарный гранулярный `RWLockManager`** («всё или ничего») | Проверка и захват всех ключей задачи выполняются атомарно под единым `asyncio.Lock`. `SHARED + SHARED` совместимы; `EXCLUSIVE` блокирует `SHARED` и `EXCLUSIVE`. При конфликте задача остаётся в `QUEUED`, не удерживая частичных ресурсов, с записью точной причины в `blocked_by`. |
| **FR-03** | **Work-conserving планировщик с Priority Aging** | Планировщик обходит очередь без *Head-of-Line Blocking* (независимые задачи стартуют мимо ожидающих) и динамически повышает приоритет ожидающих ручных задач: `P_eff = P_base - floor((t - t_due) / T_aging)`. |
| **FR-04** | **Дедупликация активных заявок и отмена `cancel()`** | Повторная заявка с тем же `(process_id, target_date)` при наличии задачи в `QUEUED`/`RUNNING` мгновенно получает статус `DEDUPLICATED`. Вызов `cancel(task_id)` переводит ожидающую/выполняющуюся задачу в `CANCELLED`. |
| **FR-05** | **Многопользовательский контекст и полный аудит в SQLite WAL** | Каждая задача хранит `initiated_by` (`analyst_1`, `analyst_2`, `manager_1`, `scheduler_bot`) и `session_id`; все переходы и решения по блокировкам фиксируются в `task_runs`, `lock_audit` и `orchestrator_events`. |
| **FR-06** | **Сравнение режимов «AS-IS (Без оркестратора)» и «TO-BE (С оркестратором)»** | Переключатель `orchestration_enabled` управляет прохождением через `RWLockManager` без изменения бизнес-логики сервисов, рассчитывает `Data Integrity Score` и сохраняет агрегаты в `simulation_runs` / `v_before_after_comparison`. |
| **FR-07** | **Нативный десктопный пульт управления и управление регламентом** | Десктопное приложение на CustomTkinter + matplotlib (`FigureCanvasTkAgg`) отображает 5 KPI, 3 колонки задач, live-диаграмму Ганта, монитор блокировок, инспектор задачи, матрицу конфликтов, настройки `ReglamentScheduler` и поддерживает мульти-оконность (`CTkToplevel`). |

---

## 3. Нефункциональные требования (NFR)

| Код | Требование | Целевой показатель |
| :--- | :--- | :--- |
| **NFR-01** | **Гарантия отсутствия Deadlock и порчи данных** | `0` Deadlock и `Data Integrity Score = 100%` на всех 5 канонических сценариях и стресс-батчах при `orchestration_enabled = True`. |
| **NFR-02** | **Защита от зависших воркеров через TTL блокировок** | При превышении `timeout_sec` (30 с по умолчанию) воркер принудительно останавливается, блокировки освобождаются с записью `EXPIRED_TTL` в `lock_audit`, задача переходит в `FAILED (TimeoutError)`. |
| **NFR-03** | **Отзывчивость нативного десктопного UI** | Неблокирующий цикл `root.after(120, ...)` поверх `asyncio`; троттлинг перерисовки `matplotlib` Ганта (300 мс); среднее время одиночной операции записи/чтения `sqlite3` WAL `< 2 мс`. |
| **NFR-04** | **Корректная мульти-оконная работа** | Независимая работа нескольких окон аналитиков (`CTk` + `CTkToplevel`) поверх единого `Orchestrator` с автоматической отпиской и закрытием сессии при закрытии окна. |
| **NFR-05** | **Автономность и запуск одной командой** | Автоматическая генерация `synthetic_data/*` (`seed=42`) и инициализация `data/orchestrator.db` при первом старте `uv run main.py` без Docker, Redis и внешней СУБД. |

---

## 4. Сравнительный анализ архитектурных альтернатив

| Вариант решения | Механизм синхронизации | Преимущества | Ограничения | Вердикт |
| :--- | :--- | :--- | :--- | :--- |
| **1. Блокировки СУБД (`2PL / SELECT FOR UPDATE`)** | Транзакционные блокировки строк/таблиц в реляционной БД | Нативный механизм СУБД, локальный ACID | Не предотвращает межсервисный AB-BA Deadlock, нет приоритетной очереди, дедупликации и прозрачности причин ожидания в UI | **Не подходит** |
| **2. Глобальный мьютекс (`FIFO`)** | Строго последовательное выполнение по одной задаче за раз | Простота реализации, 0 гонок и 0 deadlock | Полностью уничтожает параллелизм независимых дат и `SHARED`-чтений, кратно увеличивает makespan | **Неоптимально** |
| **3. Полный рефакторинг микросервисов (`MVCC` + изолированный staging)** | Переписывание всех расчётных сервисов на версионные срезы | Высокий параллелизм без внешних блокировок | Требует дорогостоящей переделки кода всех legacy-сервисов и изменения схемы хранилища | **Этап развития** |
| **4. Централизованный оркестратор (`RWLockManager` + Priority Aging)** | Атомарный захват контракта ресурсов («всё или ничего») + work-conserving очередь | Не требует изменения формул микросервисов, 100% исключает deadlock и Lost Update, сохраняет параллелизм независимых дат и даёт полный контроль в UI | В кластерном контуре требует вынесения состояния локов в распределённый координатор (Redis/PostgreSQL) | **Оптимально (выбрано)** |
