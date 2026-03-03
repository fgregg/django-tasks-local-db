from django.db.models.signals import pre_save
from django.dispatch import receiver

from .models import DBTaskResult, _get_date_max


@receiver(pre_save, sender=DBTaskResult)
def set_default_run_after(sender, instance, **kwargs):
    if instance.run_after is None:
        instance.run_after = _get_date_max()
