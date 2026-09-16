"""Бэкфилл минутных свечей под неоценённые прогнозы.

Почему отдельным скриптом, а не включением EVALUATE_DOWNLOAD в проде:
оценщик с докачкой раньше рождал лавину запросов к биржам и падал по таймлимиту
(1800 с) — именно поэтому докачку и выключили (`price_sync.py:168`). Здесь темп
задан явно, есть курсор и стоп-условия, а оценщик не трогается: он подхватит
данные сам, когда они появятся.

Единица работы — (монета, сутки): качаем день целиком, и все прогнозы этой монеты
за этот день закрываются разом. Повторный проход идемпотентен —
`ensure_minute_data_smart` сначала считает, чего не хватает.

Запуск:   python /tmp/backfill_candles.py
Прогресс: docker exec infra-celery-worker-evaluate-1 tail -f /var/lib/github_export/backfill.log
Стоп:     создать файл /var/lib/github_export/backfill.STOP
"""

import asyncio
import datetime
import sys
import os
import shutil
import time

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "application.settings")
django.setup()

from django.conf import settings  # noqa: E402
from django.db.models import Count  # noqa: E402

from application.iac import IOCContainer  # noqa: E402
from coins.models import Coin, CoinPriceHistory  # noqa: E402
from parsers.models import Forecast  # noqa: E402

WORK_DIR = "/var/lib/github_export"
LOG = f"{WORK_DIR}/backfill.log"
CURSOR = f"{WORK_DIR}/backfill.cursor"
STOP_FLAG = f"{WORK_DIR}/backfill.STOP"

PAUSE_S = 0.4           # пауза между окнами — щадящий темп к биржам
MIN_FREE_GB = 12        # ниже этого свободного места — стоп
MAX_ERRORS_IN_ROW = 10  # столько подряд ошибок = биржа не отвечает, стоп


def log(msg: str) -> None:
    line = f"[{datetime.datetime.now(datetime.UTC):%F %T UTC}] {msg}"
    print(line, flush=True)
    with open(LOG, "a") as f:
        f.write(line + "\n")


def free_gb() -> float:
    return shutil.disk_usage("/").free / 1024**3


def done_keys() -> set[str]:
    if not os.path.exists(CURSOR):
        return set()
    with open(CURSOR) as f:
        return {line.strip() for line in f if line.strip()}


def _date_arg(flag: str) -> "datetime.date | None":
    """--since/--until YYYY-MM-DD. 16.09.2026: разрешение основателя даётся на
    КОНКРЕТНЫЙ диапазон дат, а скрипт по умолчанию берёт все незалитые окна.
    Без фильтра это была бы запись в coin_price_history (97 ГБ, без бэкапа)
    шире согласованного. По умолчанию фильтра нет — прежнее поведение."""
    if flag not in sys.argv:
        return None
    value = sys.argv[sys.argv.index(flag) + 1]
    return datetime.datetime.strptime(value, "%Y-%m-%d").date()


def main() -> None:
    since, until = _date_arg("--since"), _date_arg("--until")
    # Единицы работы: (монета, сутки) по прогнозам, которые не удалось оценить.
    rows = (
        Forecast.objects.filter(status__in=["no_data_internal", "no_data_no_coverage", "pending", "none"])
        .exclude(post__published_at=None)
        .annotate(day=django.db.models.functions.TruncDate("post__published_at"))
        .values("coin_id", "day")
        .annotate(n=Count("id"))
        .order_by("-day")  # от свежего к старому: свежее полезнее для витрины
    )
    work = [(r["coin_id"], r["day"], r["n"]) for r in rows]
    if since or until:
        before_filter = len(work)
        work = [w for w in work if (since is None or w[1] >= since) and (until is None or w[1] <= until)]
        log(f"фильтр по датам {since}..{until}: окон {before_filter} -> {len(work)}")
    already = done_keys()
    todo = [w for w in work if f"{w[0]}:{w[1]}" not in already]

    log(f"=== СТАРТ. окон всего {len(work)}, уже сделано {len(already)}, к работе {len(todo)} ===")
    log(f"свободно на диске {free_gb():.1f} ГБ, пауза {PAUSE_S} с, стоп-флаг {STOP_FLAG}")

    svc = asyncio.run(IOCContainer.price_sync_service())
    settings.EVALUATE_DOWNLOAD = True  # только в этом процессе, конфиг прода не трогаем

    coins = {c.id: c for c in Coin.objects.filter(id__in={w[0] for w in todo})}
    errors_in_row = 0
    added_total = 0
    t_start = time.time()

    for i, (coin_id, day, n_forecasts) in enumerate(todo, start=1):
        if os.path.exists(STOP_FLAG):
            log("СТОП: найден стоп-флаг, останавливаюсь штатно")
            break
        if free_gb() < MIN_FREE_GB:
            log(f"СТОП: свободно {free_gb():.1f} ГБ < {MIN_FREE_GB} ГБ")
            break
        if errors_in_row >= MAX_ERRORS_IN_ROW:
            log(f"СТОП: {errors_in_row} ошибок подряд — биржа не отвечает")
            break

        coin = coins.get(coin_id)
        if coin is None:
            continue

        start = datetime.datetime.combine(day, datetime.time.min, tzinfo=datetime.UTC)
        end = start + datetime.timedelta(hours=23, minutes=59)
        before = CoinPriceHistory.objects.filter(coin=coin, timestamp__gte=start, timestamp__lte=end).count()

        try:
            svc.ensure_minute_data_smart(coin=coin, start_time=start, end_time=end)
            errors_in_row = 0
        except Exception as exc:  # биржа/сеть/лимит — не роняем весь прогон
            errors_in_row += 1
            log(f"  ошибка {coin.symbol} {day}: {type(exc).__name__}: {str(exc)[:120]}")
            time.sleep(PAUSE_S * 5)
            continue

        after = CoinPriceHistory.objects.filter(coin=coin, timestamp__gte=start, timestamp__lte=end).count()
        added_total += after - before

        with open(CURSOR, "a") as f:
            f.write(f"{coin_id}:{day}\n")

        if i % 50 == 0 or (after - before) > 2000:
            speed = i / max(time.time() - t_start, 1) * 3600
            left = (len(todo) - i) / max(speed, 1)
            log(
                f"{i}/{len(todo)} | {coin.symbol} {day}: +{after - before} свечей "
                f"(прогнозов ждало {n_forecasts}) | всего +{added_total} | "
                f"темп {speed:.0f} окон/ч, осталось ~{left:.1f} ч | диск {free_gb():.1f} ГБ"
            )

        time.sleep(PAUSE_S)

    log(f"=== ЗАВЕРШЕНО. обработано окон {i}, добавлено свечей {added_total}, диск {free_gb():.1f} ГБ ===")


if __name__ == "__main__":
    import django.db.models.functions  # noqa: F401

    main()
