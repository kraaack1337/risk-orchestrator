# RiskFlow Orchestrator — Desktop UI (`ui/`)

Нативный десктопный интерфейс диспетчерского пульта на **CustomTkinter + matplotlib (`FigureCanvasTkAgg`)** в строгом инженерном Dark Mode (`#0F1117` / `#161B22` / `#1F2630`).

## Запуск

Полный стенд с ядром `Orchestrator`, `RWLockManager` и БД `sqlite3` WAL:

```bash
uv run main.py
```

Автономный режим UI поверх встроенного `MockOrchestrator`:

```bash
python ui/app.py
```

## Архитектура модуля

- `ui/app.py` — главное окно `RiskFlowApp` (`1480x920`), мульти-оконный режим аналитиков (`CTkToplevel`), неблокирующий цикл `root.after(120, ...)` поверх `async-tkinter-loop`.
- `ui/assets/theme.py` — инженерная палитра Dark Mode и единая семантическая схема статусов.
- `ui/components/control_panel.py` — верхний `CommandToolbar` (`48px`) с переключателем `AS-IS / TO-BE`, выбором сессии, тумблером авторегламента, паузой/сбросом + Блок A (5 карточек сценариев в 1 клик).
- `ui/components/manual_form.py` — Блок B компактного ручного запуска процессов.
- `ui/components/dashboard.py` — 5 KPI-карточек (`64px`) и 3 колонки задач (`RUNNING`, `QUEUED` с `blocked_by` и отменой `×`, `ЗАВЕРШЕНЫ / СБОИ`).
- `ui/components/gantt_chart.py` — бесшовная диаграмма Ганта на `matplotlib` (`FigureCanvasTkAgg`) с фазами ожидания и выполнения.
- `ui/components/rules_view.py` — вкладка «Матрица конфликтов и Регламент» и история прогонов `AS-IS vs TO-BE`.
- `ui/components/event_log.py` — правый инспектор (`340px`): монитор `RWLockManager`, инспектор выбранной задачи (JSON + ключи блокировок + ошибки) и живой моноширинный журнал событий.
