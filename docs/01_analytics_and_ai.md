# Аналитика предметной области и ИИ-анализ узких мест (AS-IS)

## 1. Описание бизнес-процесса (AS-IS)

Рассматривается контур оперативной оценки логистических рисков федеральной торговой сети:
- **Инфраструктура:** 5 распределительных складов (`WH-MSK-01`, `WH-SPB-02`, `WH-KZN-03`, `WH-EKB-04`, `WH-KRR-05`), 4 магистральных транспортных коридора (`М-11`, `М-12`, `Р-242`, `М-4`), 8 товарных позиций SKU (включая скоропорт и фарму холодовой цепи) и 6 транспортных компаний (`CAR-01`..`CAR-06`).
- **Микросервисы расчёта рисков (4 воркера):**
  1. `StockoutRisk` — риск дефицита складских запасов.
  2. `DelayRisk` — риск срыва сроков транзитных поставок.
  3. `SpoilageRisk` — риск порчи/списания скоропортящихся товаров.
  4. `CarrierDefaultRisk` — скользящий рейтинг надёжности и риск невыхода перевозчика.
- **Общие витрины данных (4 источника + 1 staging-буфер):**
  - `WarehouseStocks` (партиционирована по дате `business_date`),
  - `TransitTelemetry` (партиционирована по дате `business_date`),
  - `WeatherAndTraffic` (справочник погоды и трафика по дате `business_date`),
  - `SupplierSLA` (глобальная кумулятивная витрина за 30 дней без партиционирования по дате),
  - `staging_carrier_sla_rolling` (общая промежуточная таблица агрегации скользящего SLA).

В архитектуре **AS-IS** автоматический планировщик (`scheduler_bot`) и риск-аналитики (`analyst_1`, `analyst_2`, `manager_1`) запускают микросервисы напрямую без централизованного диспетчера ресурсов, что приводит к регулярным блокировкам, потере обновлений и искажению расчётов.

---

## 2. Синтетический журнал сбоев старой системы (`seed=42`)

Скрипт `scripts/generate_legacy_logs.py --seed 42` формирует воспроизводимый суточный журнал в `synthetic_data/legacy_system_logs.log` и `synthetic_data/legacy_system_logs.jsonl` (**86 событий**: `51 INFO`, `14 WARN`, `21 ERROR`).

Фрагмент исходного журнала (`legacy_system_logs.log`):

```text
2026-11-20T06:00:08.532Z [INFO ] job=#104 svc=StockoutRisk       trig=AUTO   user=scheduler_bot date=2026-11-20 action=SNAPSHOT_READ | Job #104 (AUTO reglament) read WarehouseStocks(2026-11-20) row_version=1
2026-11-20T06:00:10.315Z [INFO ] job=#105 svc=SpoilageRisk       trig=MANUAL user=analyst_1     date=2026-11-20 action=SNAPSHOT_READ | Job #105 (MANUAL by analyst_1) overlapped reglament and read stale WarehouseStocks(2026-11-20) row_version=1
2026-11-20T06:00:14.702Z [ERROR] job=#105 svc=SpoilageRisk       trig=MANUAL user=analyst_1     date=2026-11-20 action=OVERWRITE_CONFLICT code=ERR_LOST_UPDATE | DataCorruptionError: Lost Update detected! Job #105 overwrote WarehouseStocks(2026-11-20) from version=1->3, erasing stockout_risk_flag written by Job #104
2026-11-20T06:00:19.120Z [INFO ] job=#107 svc=StockoutRisk       trig=AUTO   user=scheduler_bot date=2026-11-20 action=LOCK_ACQUIRE | Job #107 acquired EXCLUSIVE lock on WarehouseStocks(2026-11-20)
2026-11-20T06:00:20.411Z [INFO ] job=#108 svc=DelayRisk          trig=MANUAL user=analyst_2     date=2026-11-20 action=LOCK_ACQUIRE | Job #108 acquired EXCLUSIVE lock on TransitTelemetry(2026-11-20)
2026-11-20T06:00:24.890Z [ERROR] job=#107 svc=StockoutRisk       trig=AUTO   user=scheduler_bot date=2026-11-20 action=ABORT_DEADLOCK code=ERR_DEADLOCK_ABBA | DeadlockError: cyclic wait detected between Job #107 (StockoutRisk) and Job #108 (DelayRisk) on WarehouseStocks<->TransitTelemetry (2026-11-20)
```

---

## 3. Промпт для ИИ-анализа узких мест

```text
Проанализируй фрагмент логов системы оценки рисков (86 событий). Сделай количественную сводку ошибок по типам (ERR_DEADLOCK_ABBA, ERR_LOST_UPDATE, ERR_CROSS_DATE_STAGING, ERR_DIRTY_READ_GLOBAL). Проведи Root Cause Analysis, выяви узкие места и сформулируй системные требования к оркестратору.
```

---

## 4. Структурированный вывод ИИ-анализа

### 4.1. Количественная сводка инцидентов
- Всего записей в журнале: **86 событий** (`51 INFO`, `14 WARN`, `21 ERROR`).
- Доля аварийных задач: **21 сбой** из запущенных в конфликтном окне задач (**~55,3%** отказов и нарушений целостности данных):

| Код ошибки | Класс инцидента | Кол-во ERROR | Доля от всех ошибок | Затронутые сервисы |
| :--- | :--- | :---: | :---: | :--- |
| `ERR_DEADLOCK_ABBA` | Взаимная блокировка AB-BA (`DeadlockError`) | **8** (4 пары) | 38,1% | `StockoutRisk` ↔ `DelayRisk` |
| `ERR_CROSS_DATE_STAGING` | Коллизия общего staging-буфера за разные даты (`CrossDateCollisionError`) | **6** (3 пары) | 28,6% | `CarrierDefaultRisk` (разные даты) |
| `ERR_LOST_UPDATE` | Потеря обновления при гонке AUTO и MANUAL (`DataCorruptionError`) | **4** | 19,0% | `StockoutRisk` + `SpoilageRisk` |
| `ERR_DIRTY_READ_GLOBAL` | Грязное чтение глобальной витрины SLA (`DataCorruptionError`) | **3** | 14,3% | `CarrierDefaultRisk` → `DelayRisk` |

### 4.2. Анализ первопричин (Root Cause Analysis)
1. **AB-BA Deadlock:** `StockoutRisk` захватывает ресурсы в порядке `WarehouseStocks (WRITE) -> TransitTelemetry (READ)`, а `DelayRisk` — в обратном порядке `TransitTelemetry (WRITE) -> WarehouseStocks (READ)`. При одновременном запуске за одну расчётную дату возникает цикл ожидания (условие *Hold and Wait* + *Circular Wait*).
2. **Lost Update (гонка регламента и ручного запуска):** `StockoutRisk` (AUTO) и `SpoilageRisk` (MANUAL) параллельно читают `WarehouseStocks` с одинаковым `row_version=1` и последовательно перезаписывают витрину, затирая рассчитанные флаги друг друга.
3. **Cross-Date Staging Collision:** `CarrierDefaultRisk` использует общую непартиционированную таблицу `staging_carrier_sla_rolling` при расчёте 30-дневного скользящего окна — параллельный запуск за вчера и сегодня перемешивает строки в буфере.
4. **Грязное чтение глобального SLA:** витрина `SupplierSLA` не партиционирована по датам (`GLOBAL_CUMULATIVE`), поэтому чтение `SupplierSLA` сервисом `DelayRisk` во время перезаписи со стороны `CarrierDefaultRisk` за любую дату приводит к чтению несогласованного среза.

### 4.3. Узкие места архитектуры AS-IS (Hotspots)
- Витрина `WarehouseStocks` на пиковых датах (`2026-11-20`, `2026-11-21`) — точка пересечения 3 сервисов (`StockoutRisk`, `SpoilageRisk`, `DelayRisk`).
- Глобальная витрина `SupplierSLA` и буфер `staging_carrier_sla_rolling` — межсуточные узкие места, связывающие задачи за разные расчётные даты.
- Отсутствие дедупликации повторных пользовательских кликов поверх уже работающего регламента.

### 4.4. Вытекающие требования к целевому оркестратору (TO-BE)
1. Внедрить гранулярный `RWLockManager` с режимами `SHARED` / `EXCLUSIVE` и областями `BY_DATE` (для партиционированных витрин) и `GLOBAL` (для `SupplierSLA` и staging-буферов).
2. Исключить условие *Hold and Wait* за счёт **атомарного захвата всего набора ключей до старта задачи** («всё или ничего») под единым `asyncio.Lock`.
3. Реализовать work-conserving очередь с **Priority Aging** (обход заблокированных задач без *Head-of-Line Blocking* + защита ручных запросов от голодания) и дедупликацией активных дублей (`DEDUPLICATED`).
