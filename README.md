# RiskFlow Orchestrator — Система бесконфликтной оркестрации процессов оценки логистических рисков

**RiskFlow Orchestrator** — нативное десктопное приложение на Python (**CustomTkinter + matplotlib `FigureCanvasTkAgg` + asyncio + sqlite3 WAL**) для управления конкурентным доступом и оркестрации параллельных задач оценки рисков в логистике и цепях поставок.

---

## Оглавление

1. [Проблема и предметная область (AS-IS)](#1-проблема-и-предметная-область-as-is)
2. [Архитектура решения (TO-BE)](#2-архитектура-решения-to-be)
3. [Матрица зависимостей и конфликтов](#3-матрица-зависимостей-и-конфликтов)
4. [Подробная инструкция по установке](#4-подробная-инструкция-по-установке)
5. [Запуск приложения](#5-запуск-приложения)
6. [Гайд по работе с интерфейсом](#6-гайд-по-работе-с-интерфейсом)
7. [Структура репозитория](#7-структура-репозитория)
8. [Тестирование и проверка качества кода](#8-тестирование-и-проверка-качества-кода)
9. [Решение частых проблем](#9-решение-частых-проблем)

---

## 1. Проблема и предметная область (AS-IS)

В контуре оценки логистических рисков работают **4 расчётных микросервиса** поверх **4 общих витрин данных** и одного промежуточного staging-буфера:

- **Микросервисы:**
  - `StockoutRisk` — расчёт риска дефицита складских остатков;
  - `DelayRisk` — расчёт риска срыва сроков транзитных поставок;
  - `SpoilageRisk` — расчёт риска порчи и списания скоропортящихся товаров;
  - `CarrierDefaultRisk` — расчёт скользящего рейтинга надёжности перевозчиков.
- **Витрины данных:**
  - `WarehouseStocks` (партиционирована по дате `business_date`);
  - `TransitTelemetry` (партиционирована по дате `business_date`);
  - `WeatherAndTraffic` (справочник погоды и трафика по дате `business_date`);
  - `SupplierSLA` (глобальная кумулятивная витрина за 30 дней, без партиционирования по датам);
  - `staging_carrier_sla_rolling` (общий staging-буфер агрегации скользящего SLA).

Без централизованного диспетчера параллельные запуски от автоматического планировщика (`scheduler_bot`, `AUTO`) и риск-аналитиков (`analyst_1`, `analyst_2`, `manager_1`, `MANUAL`) приводят к трём классам сбоев:

1. **Взаимные блокировки (`DeadlockError` / AB-BA Deadlock):** `StockoutRisk` захватывает сначала `WarehouseStocks`, затем `TransitTelemetry`, а `DelayRisk` — в обратном порядке (`TransitTelemetry` → `WarehouseStocks`).
2. **Порча данных и гонки (`DataCorruptionError` / Lost Update & Dirty Read):** наложение ручных запусков на регламент поверх `WarehouseStocks` (перетирание `row_version`) или чтение глобальной `SupplierSLA` во время её перезаписи.
3. **Коллизии за разные даты (`CrossDateCollisionError`):** одновременный запуск `CarrierDefaultRisk` за вчера и сегодня перемешивает строки в общем staging-буфере `dom_sla_staging_buffer`.

По результатам ИИ-анализа синтетического журнала старой системы (`86` событий, `seed=42`) в режиме AS-IS аварийно завершалось **55,3%** запусков (`8` AB-BA Deadlock, `6` коллизий staging-буфера, `4` Lost Update, `3` грязных чтения глобального SLA).

---

## 2. Архитектура решения (TO-BE)

Вся система работает в одном процессе Python без внешних брокеров, Docker и сторонних СУБД:

```text
[UI CustomTkinter: Сценарии / Форма / CTkToplevel] ──┐
                                                     ├──► Orchestrator.submit() ──► Storage (sqlite3 WAL)
[ReglamentScheduler (Фоновый таймер)] ──────────────┘             │
                                                                  ▼
                                                     Режим оркестрации включён?
                                                       ├── НЕТ (AS-IS) ──► Прямой запуск воркеров
                                                       │                   (DeadlockError / Lost Update / Staging)
                                                       │
                                                       └── ДА (TO-BE)  ──► Work-conserving очередь + Priority Aging
                                                                                 │
                                                                                 ▼
                                                                      RWLockManager.try_acquire() («всё или ничего»)
                                                                        ├── Занято   ──► QUEUED (причина blocked_by, доступен cancel)
                                                                        └── Свободно ──► Атомарный захват (SHARED/EXCLUSIVE) ──► RUNNING
                                                                                                                      │
                                                                                             BaseRiskService.run() ◄──┘
                                                                                                                      │
                                                                                                                      ▼
                                                                                                   COMPLETED ──► RWLockManager.release()
                                                                                                                      │
                                                                                                                      ▼
                                                                                                   Будит планировщик (asyncio.Event)
                                                                                                   + обновляет KPI, Гант и Монитор локов
```

### Ключевые механизмы ядра
- **`RWLockManager` ([`core/lock_manager.py`](core/lock_manager.py)):**
  - Атомарный захват всего набора ключей под единым `asyncio.Lock` («всё или ничего» — устраняет условие *Hold and Wait*, делая deadlock невозможным).
  - Поддержка совместимости **`SHARED + SHARED`** (параллельное чтение) и эксклюзивности **`EXCLUSIVE`** (монопольная запись).
  - Области блокировок **`BY_DATE`** (независимые даты одного и того же совместимого процесса выполняются параллельно) и **`GLOBAL`** (для `SupplierSLA` и staging-буферов).
  - Автоматическое снятие зависших блокировок по TTL (`timeout_sec`, аудит `EXPIRED_TTL`).
- **Work-conserving планировщик ([`core/orchestrator.py`](core/orchestrator.py)):**
  - Обходит очередь без *Head-of-Line Blocking*: независимые задачи стартуют мимо заблокированных.
  - **Priority Aging:** динамический приоритет `P_eff = P_base - floor((t - t_due) / T_aging)` защищает ручные задачи от голодания.
  - **Дедупликация (`DEDUPLICATED`):** повторная заявка `(process_id, target_date)` при наличии активной задачи в `QUEUED`/`RUNNING` мгновенно отсекается без лишних вычислений.
  - **Отмена (`cancel(task_id)`):** перевод ожидающей или выполняющейся задачи в `CANCELLED` с корректным освобождением ресурсов.
- **Хранилище и аудит ([`core/storage.py`](core/storage.py), [`core/schema.sql`](core/schema.sql)):**
  - Встроенный `sqlite3` в режиме `WAL` (`isolation_level=None`, `PRAGMA foreign_keys = ON`, `busy_timeout = 5000`).
  - `15` нормализованных таблиц, `10` индексов и `3` аналитических представления (`v_gantt_timeline`, `v_before_after_comparison`, `v_conflict_matrix`).

---

## 3. Матрица зависимостей и конфликтов

| Микросервис | Читает (`SHARED`) | Пишет (`EXCLUSIVE`) | Область блокировок | `allow_parallel_dates` | Staging-ключ |
| :--- | :--- | :--- | :--- | :---: | :--- |
| **`StockoutRisk`** | `TransitTelemetry`, `SupplierSLA` | `WarehouseStocks` | `WarehouseStocks`, `TransitTelemetry`: `BY_DATE`; `SupplierSLA`: `GLOBAL` | `False` | — |
| **`DelayRisk`** | `WarehouseStocks`, `WeatherAndTraffic`, `SupplierSLA` | `TransitTelemetry` | `TransitTelemetry`, `WarehouseStocks`, `WeatherAndTraffic`: `BY_DATE`; `SupplierSLA`: `GLOBAL` | `True` | — |
| **`SpoilageRisk`** | `WeatherAndTraffic` | `WarehouseStocks` (`READ_WRITE`) | `BY_DATE` | `True` | — |
| **`CarrierDefaultRisk`** | `TransitTelemetry`, `WeatherAndTraffic` | `SupplierSLA` | `SupplierSLA`: `GLOBAL`; `TransitTelemetry`, `WeatherAndTraffic`: `BY_DATE` | `False` | `staging_carrier_sla_rolling` |

---

## 4. Подробная инструкция по установке

### Шаг 1. Клонирование репозитория

```bash
git clone <https://github.com/kraaack1337/risk-orchestrator.git>
cd risk-orchestrator
```

### Шаг 2. Установка системного Tcl/Tk (для нативного окна CustomTkinter)

- **Arch Linux / CachyOS / Manjaro:**
  ```bash
  sudo pacman -S tk
  ```
- **Ubuntu / Debian / Linux Mint:**
  ```bash
  sudo apt update && sudo apt install -y python3-tk
  ```
- **Fedora / RHEL:**
  ```bash
  sudo dnf install -y python3-tkinter
  ```
- **Windows / macOS:** официальный установщик Python с `python.org` уже включает Tcl/Tk. Если на macOS используется Python из Homebrew, выполните `brew install python-tk`.

Проверьте, что Tkinter работает:

```bash
python3 -c "import tkinter; print('Tk version:', tkinter.TkVersion)"
```

### Шаг 3. Установка зависимостей Python

#### Способ A — через `uv` (рекомендуемый)

1. Установите `uv`, если он ещё не установлен:
   - **Arch / CachyOS:** `sudo pacman -S uv`
   - **Linux / macOS:** `curl -LsSf https://astral.sh/uv/install.sh | sh`
   - **Windows (PowerShell):** `powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"`
2. Синхронизируйте окружение (создаст `.venv` и установит `customtkinter`, `matplotlib`, `async-tkinter-loop`, `pytest`, `ruff`):
   ```bash
   uv sync
   ```

#### Способ B — через стандартный `venv` и `pip`

```bash
python3 -m venv .venv

# Активация окружения:
source .venv/bin/activate          # Linux / macOS (bash/zsh)
# source .venv/bin/activate.fish   # Linux / macOS (fish)
# .venv\Scripts\Activate.ps1       # Windows PowerShell

pip install --upgrade pip
pip install -r requirements.txt
```

Для запуска тестов и линтера также установите dev-инструменты:

```bash
pip install pytest ruff
```

---

## 5. Запуск приложения

### Полный рабочий стенд (Ядро + RWLockManager + SQLite WAL + Синтетика)

```bash
# Через uv:
uv run main.py

# Либо напрямую из .venv:
.venv/bin/python main.py
```

При первом запуске `main.py` автоматически:
1. Проверит наличие `synthetic_data/*.csv`, `synthetic_data/legacy_system_logs.*` и `synthetic_data/scenarios.json` — при отсутствии сгенерирует их с `seed=42`.
2. Создаст БД `data/orchestrator.db` по схеме `core/schema.sql` и загрузит доменные витрины `dom_*`.
3. Откроет нативное окно **RiskFlow Orchestrator** (`1600×980`, с авто-масштабированием на 2K/HiDPI экранах).

### Дополнительные режимы запуска

```bash
# Автономный режим UI (только интерфейс поверх встроенного MockOrchestrator):
.venv/bin/python ui/app.py

# Только инициализация синтетики и БД без открытия GUI:
.venv/bin/python main.py --bootstrap-only

# Перегенерация синтетических данных с нуля (seed=42):
rm -rf data/ synthetic_data/
.venv/bin/python main.py --bootstrap-only
```

---

## 6. Гайд по работе с интерфейсом

Окно приложения выполнено в строгом инженерном Dark Mode (`#0F1117` / `#161B22` / `#1F2630`) и разделено на **4 функциональные зоны**:

### Зона 1 — Верхний Command Toolbar
- **Слева:** логотип `RISKFLOW // ORCHESTRATOR` и сегментированный переключатель режима **`[БЕЗ ОРКЕСТРАТОРА (AS-IS) | С ОРКЕСТРАТОРОМ (TO-BE)]`**.
  - При выборе `AS-IS` тулбар подсвечивается красной рамкой `#F85149` — задачи отправляются напрямую к воркерам без `RWLockManager`.
  - При выборе `TO-BE` включается защита очереди, блокировок и дедупликации.
- **По центру:** выбор профиля текущей вкладки/окна (`analyst_1`, `analyst_2`, `manager_1`) и кнопка **`+ Новое окно аналитика`** (открывает второе независимое окно `CTkToplevel` поверх единого экземпляра `Orchestrator`).
- **Справа:**
  - тумблер **`Авторегламент (15с)`** — включает/выключает фоновый [`ReglamentScheduler`](core/scheduler.py);
  - кнопка **`Пауза / Продолжить`** — приостанавливает диспетчеризацию очереди для наглядного осмотра `QUEUED`;
  - кнопка **`Сбросить стенд`** — останавливает текущие расчёты, очищает очередь, блокировки, `SharedDataLayer` и таблицы рантайма SQLite.

### Зона 2 — Левый Dock-сайдбар
- **Блок A «Сценарии проверки (1 клик)»:**
  1. **1. Ручной vs Регламент** — `StockoutRisk` (`AUTO`) + `SpoilageRisk` (`MANUAL`) + дубль `StockoutRisk` за `2026-11-20` (проверка Lost Update и дедупликации).
  2. **2. AB-BA Deadlock** — одновременный запуск `StockoutRisk` и `DelayRisk` за `2026-11-20` (встречный захват `WarehouseStocks ↔ TransitTelemetry`).
  3. **3. Перекрёстные даты и Global SLA** — два `CarrierDefaultRisk` за `11-19` и `11-20` (staging-буфер) + `DelayRisk` за `11-21` (глобальное чтение `SupplierSLA`).
  4. **4. Безопасный параллелизм** — `SpoilageRisk` (`11-18`, `11-19`) + `DelayRisk` (`11-22`), стартующие строго одновременно (`max_parallelism = 3`).
  5. **5. Шторм пользователей — 10 задач** — одновременный вброс задач от регламента и 3 аналитиков.
- **Блок B «Ручной запуск процесса»:**
  - выбор сервиса, просмотр его контракта (`EXCL(W)`, `SHRD(R)`, `PARALLEL_DATES` / `STAGING_LOCK`),
  - выбор даты (`2026-11-18`..`2026-11-22`), приоритета (`1 Высокий` / `2 Обычный` / `3 Фоновый`) и кнопка **`Запустить задачу`**.

### Зона 3 — Центральная рабочая область (2 вкладки)
- **Вкладка «Диспетчер и Таймлайн»:**
  - **5 KPI-карточек:** `Всего задач`, `Активны / В очереди`, `Deadlock / Ошибки`, `Дедуплицировано`, `Data Integrity Score (%)`.
  - **3 колонки статусов:**
    - `В РАБОТЕ (RUNNING)` — живой таймер и прогресс-бар `6px`;
    - `В ОЧЕРЕДИ (QUEUED)` — динамический приоритет `Peff`, янтарная плашка `blocked_by` и кнопка отмены `×`;
    - `ЗАВЕРШЕНЫ / СБОИ` — время ожидания/работы и красная плашка исключения при ошибке.
  - **Live-диаграмма Ганта (`matplotlib`):** составные горизонтальные бары (ожидание в очереди + выполнение). Клик по карточке задачи или бару Ганта выбирает задачу в Инспекторе.
- **Вкладка «Матрица конфликтов и Регламент»:**
  - настройка интервала `ReglamentScheduler` (`5 / 10 / 15 / 30 сек`);
  - таблица ресурсных контрактов процессов (`READ`/`WRITE`, `BY_DATE`/`GLOBAL`, `allow_parallel_dates`, `staging_key`, `TTL`);
  - матрица попарных конфликтов `4×4` и история прогонов `AS-IS vs TO-BE` из `v_before_after_comparison`.

### Зона 4 — Правый инспектор ресурсов и логов
- **Монитор блокировок (`RWLockManager`):** статус 4 витрин и `Staging-буфера` в реальном времени (`[FREE]`, `[SHARED: #...]`, `[EXCLUSIVE: #...]`).
- **Инспектор выбранной задачи:** ключи `RWLockManager`, причина блокировки, текст исключения и JSON-результат расчёта риска.
- **Живой журнал событий:** моноширинная консоль с цветовой подсветкой уровней `INFO` / `WARN` / `ERROR`.

---

## 7. Структура репозитория

```text
risk-orchestrator/
├── main.py                        # Точка входа: автоинициализация данных, сборка ядра и запуск UI
├── pyproject.toml                 # Конфигурация uv, ruff и pytest
├── requirements.txt               # Список зависимостей для pip
├── .python-version                # Версия Python
├── core/                          # Ядро оркестратора (Артём)
│   ├── models.py                  # Контракты: Enums, ProcessSpec, TaskRequest, TaskRun, ошибки
│   ├── lock_manager.py            # RWLockManager (SHARED/EXCLUSIVE, BY_DATE/GLOBAL, аудит)
│   ├── orchestrator.py            # Work-conserving планировщик, Priority Aging, cancel(), метрики
│   ├── scheduler.py               # Фоновый ReglamentScheduler
│   ├── storage.py                 # Слой доступа к sqlite3 WAL
│   └── schema.sql                 # DDL-схема (15 таблиц, 10 индексов, 3 VIEW)
├── services/                      # Сервисы и сценарии (Роман)
│   ├── microservices.py           # SharedDataLayer + 4 эмулятора с проверкой row_version
│   ├── scenarios.py               # Загрузка 5 канонических сценариев и генератор батчей
│   └── mocks.py                   # MockOrchestrator и FakeRiskService для автономного UI
├── scripts/                       # Генераторы синтетики с seed=42 (Александр, Роман)
│   ├── generate_domain_data.py    # 316 строк CSV-витрин + загрузка в sqlite3
│   ├── generate_legacy_logs.py    # 86 событий логов старой системы (LOG + JSONL)
│   └── generate_scenarios.py      # Генерация scenarios.json
├── ui/                            # Нативный десктопный UI (Максим)
│   ├── app.py                     # Главное окно, CTkToplevel, цикл root.after(120, ...)
│   ├── contracts.py               # Контрактный мост UI <-> Core
│   ├── assets/theme.py            # Палитра Dark Mode и шрифты
│   └── components/                # Виджеты (Toolbar, Сценарии, Форма, KPI+Колонки, Гант, Инспектор, Матрица)
├── synthetic_data/                # Сгенерированные CSV-витрины, логи и scenarios.json
├── docs/                          # Аналитика, системные требования, дизайн и промпт презентации
└── tests/                         # Тесты ядра, SQLite-схемы и интеграции UI/сценариев
```

---

## 8. Тестирование и проверка качества кода

```bash
# Проверка линтером Ruff:
.venv/bin/ruff check .

# Запуск всех тестов (21 тест: LockManager, Orchestrator, Storage/SQLite, 5 сценариев AS-IS/TO-BE, UI view-models):
.venv/bin/pytest -v
```

---

## 9. Решение частых проблем

- **`ImportError: libtk8.6.so: cannot open shared object file`**
  Установите системный пакет `tk` (`sudo pacman -S tk` на Arch/CachyOS или `sudo apt install python3-tk` на Ubuntu/Debian).
- **`uv: command not found`**
  Запускайте напрямую через виртуальное окружение: `.venv/bin/python main.py`, либо установите `uv` (`sudo pacman -S uv`).
- **Необходимо очистить историю прогонов и начать с чистого листа**
  Нажмите кнопку **`Сбросить стенд`** в правом верхнем углу приложения или удалите папку `data/` перед запуском.
