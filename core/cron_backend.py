from apscheduler.schedulers.background import BackgroundScheduler


def init_cron_backend():
    _cron_scheduler = BackgroundScheduler()
    _cron_scheduler.start()
    return _cron_scheduler


cron_scheduler = init_cron_backend()
