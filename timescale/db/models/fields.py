from django.db.models import DateTimeField, CharField


class TimescaleDateTimeField(DateTimeField):
    def __init__(self, *args, interval, **kwargs):
        self.interval = interval
        super().__init__(*args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        kwargs['interval'] = self.interval

        return name, path, args, kwargs

class TimescalePartitioningField(CharField):
    def __init__(self, *args, number_partitions, **kwargs):
        self.number_partitions = number_partitions
        super().__init__(*args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        kwargs['number_partitions'] = self.number_partitions

        return name, path, args, kwargs