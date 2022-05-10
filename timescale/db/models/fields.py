from django.db.models import DateTimeField, CharField, IntegerField, ForeignKey


class TimescaleDateTimeField(DateTimeField):
    def __init__(self, *args, interval, **kwargs):
        self.interval = interval
        super().__init__(*args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        kwargs['interval'] = self.interval

        return name, path, args, kwargs

class TimescalePartitioningField(object):
    def __init__(self, number_partitions):
        super().__init__()
        self.number_partitions = number_partitions

class TimescalePartitioningCharField(TimescalePartitioningField, CharField):
    def __init__(self, *args, number_partitions, **kwargs):
        TimescalePartitioningField.__init__(self, number_partitions)
        CharField.__init__(*args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = CharField.deconstruct()
        kwargs['number_partitions'] = self.number_partitions

        return name, path, args, kwargs

class TimescalePartinioningIntegerField(TimescalePartitioningField, IntegerField):
    def __init__(self, *args, number_partitions, **kwargs):
        TimescalePartitioningField.__init__(self, number_partitions)
        IntegerField.__init__(*args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = IntegerField.deconstruct()
        kwargs['number_partitions'] = self.number_partitions

        return name, path, args, kwargs

class TimescalePartinioningForeignKeyField(TimescalePartitioningField, ForeignKey):
    def __init__(self, *args, number_partitions, **kwargs):
        TimescalePartitioningField.__init__(self, number_partitions)
        ForeignKey.__init__(*args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = ForeignKey.deconstruct()
        kwargs['number_partitions'] = self.number_partitions

        return name, path, args, kwargs
